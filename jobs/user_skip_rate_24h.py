"""Compute user_skip_rate_24h in continuous or bounded replay mode."""

import argparse
import json
from datetime import UTC, datetime, timedelta

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.contracts.feature_time import right_closed_timestamp_ms
from featureforge_stream.reference.user_play_count import EventIdentityConflictError

BOOTSTRAP_SERVERS = "kafka:29092"
INPUT_TOPIC = "interaction-events.validated.v1"
FEATURE_TOPIC = "features.user-skip-rate-24h.v1"
LATE_TOPIC = "interaction-events.user-skip-rate-24h.late.v1"

WATERMARK_DELAY_MINUTES = 10
ALLOWED_LATENESS_MS = 30 * 60 * 1000
DEDUPE_TTL_HOURS = 48

LATE_EVENTS = OutputTag("user-skip-rate-24h-late", Types.STRING())
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def utc_timestamp(milliseconds: int) -> str:
    timestamp = EPOCH + timedelta(milliseconds=milliseconds)
    return timestamp.isoformat().replace("+00:00", "Z")


def event_id(payload: str) -> str:
    return json.loads(payload)["event_id"]


def user_id(payload: str) -> str:
    return json.loads(payload)["user_id"]


class FeatureTimestampAssigner(TimestampAssigner):
    """Preserve the reference's right-closed window membership."""

    def extract_timestamp(self, value, record_timestamp):
        event = InteractionEventV1.model_validate_json(value)
        return right_closed_timestamp_ms(event.event_time)


class DeduplicateEvents(KeyedProcessFunction):
    """Deduplicate by event_id within a processing-time retention horizon.

    Expiry is measured from the first accepted delivery, not event time.
    Repeated deliveries do not extend retention. A replay after expiry can
    be accepted again, including while an event-time window remains open.
    """

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor(
            "logical-event-payload-v1",
            Types.STRING(),
        )
        descriptor.enable_time_to_live(
            StateTtlConfig.new_builder(Time.hours(DEDUPE_TTL_HOURS))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        self.logical_payload = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        event = InteractionEventV1.model_validate_json(value)

        logical_payload = json.dumps(
            event.model_dump(mode="json", exclude={"ingested_at"}),
            sort_keys=True,
            separators=(",", ":"),
        )
        previous_payload = self.logical_payload.value()

        if previous_payload is None:
            self.logical_payload.update(logical_payload)
            yield value
            return

        if previous_payload != logical_payload:
            raise EventIdentityConflictError(
                f"Conflicting logical payload for event_id={event.event_id}"
            )

        # Repeated delivery: do not emit or prolong the retention period.


class CountPlaysAndSkips(AggregateFunction):
    """Retain independent play and skip counts for each user/window."""

    def create_accumulator(self):
        return (0, 0)

    def add(self, value, accumulator):
        event_type = json.loads(value)["event_type"]
        plays, skips = accumulator
        return (
            plays + int(event_type == "play"),
            skips + int(event_type == "skip"),
        )

    def get_result(self, accumulator):
        return accumulator

    def merge(self, left, right):
        return (left[0] + right[0], left[1] + right[1])


class EmitFeature(ProcessWindowFunction):
    """Attach entity and reference-compatible window metadata."""

    def process(self, key, context, elements):
        window = context.window()
        plays, skips = next(iter(elements))
        denominator = plays + skips

        yield json.dumps(
            {
                "feature_schema_version": "v1",
                "feature_name": "user_skip_rate_24h",
                "user_id": key,
                "observation_time": utc_timestamp(window.end),
                "window_start_exclusive": utc_timestamp(window.start),
                "play_count": plays,
                "skip_count": skips,
                "skip_rate": skips / denominator if denominator else 0.0,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def late_event_record(payload: str) -> str:
    return json.dumps(
        {
            "error_schema_version": "v1",
            "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
            "feature_name": "user_skip_rate_24h",
            "raw_payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def kafka_sink(topic: str, *, bootstrap_servers: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute minute-aligned user_skip_rate_24h features."
    )
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--input-topic", default=INPUT_TOPIC)
    parser.add_argument("--feature-topic", default=FEATURE_TOPIC)
    parser.add_argument("--late-topic", default=LATE_TOPIC)
    parser.add_argument(
        "--group-id",
        default="featureforge-stream-user-skip-rate-24h-v1",
    )
    parser.add_argument(
        "--job-name",
        default="featureforge-stream-user-skip-rate-24h",
    )
    parser.add_argument("--bounded", action="store_true")
    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)

    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(arguments.bootstrap_servers)
        .set_topics(arguments.input_topic)
        .set_group_id(arguments.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    )

    if arguments.bounded:
        source_builder.set_bounded(KafkaOffsetsInitializer.latest())

    raw_events = env.from_source(
        source_builder.build(),
        WatermarkStrategy.no_watermarks(),
        "validated-interaction-events-source",
    )

    timed_events = raw_events.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_minutes(WATERMARK_DELAY_MINUTES)
        ).with_timestamp_assigner(FeatureTimestampAssigner())
    )

    unique_events = (
        timed_events.key_by(event_id, key_type=Types.STRING())
        .process(DeduplicateEvents(), output_type=Types.STRING())
        .name("deduplicate-by-event-id")
        .uid("user-skip-rate-24h-deduplicate-v1")
    )

    features = (
        unique_events.key_by(user_id, key_type=Types.STRING())
        .window(
            SlidingEventTimeWindows.of(
                Time.hours(24),
                Time.minutes(1),
            )
        )
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .side_output_late_data(LATE_EVENTS)
        .aggregate(
            CountPlaysAndSkips(),
            EmitFeature(),
            accumulator_type=Types.TUPLE([Types.LONG(), Types.LONG()]),
            output_type=Types.STRING(),
        )
        .name("user-skip-rate-24h-windows")
        .uid("user-skip-rate-24h-windows-v1")
    )

    features.sink_to(
        kafka_sink(
            arguments.feature_topic,
            bootstrap_servers=arguments.bootstrap_servers,
        )
    ).name("user-skip-rate-24h-kafka-sink")

    features.get_side_output(LATE_EVENTS).map(
        late_event_record,
        output_type=Types.STRING(),
    ).sink_to(
        kafka_sink(
            arguments.late_topic,
            bootstrap_servers=arguments.bootstrap_servers,
        )
    ).name("late-events-kafka-sink")

    env.execute(arguments.job_name)


if __name__ == "__main__":
    main()
