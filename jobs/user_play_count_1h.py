"""Compute user_play_count_1h at minute-aligned observation times."""

import json
from datetime import UTC, datetime, timedelta

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
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
FEATURE_TOPIC = "features.user-play-count-1h.v1"
LATE_TOPIC = "interaction-events.user-play-count-1h.late.v1"

WATERMARK_DELAY_MINUTES = 10
ALLOWED_LATENESS_MS = 30 * 60 * 1000
DEDUPE_TTL_HOURS = 24

LATE_EVENTS = OutputTag("user-play-count-1h-late", Types.STRING())
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
    """Deduplicate globally by event_id within the configured state TTL."""

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


class CountPlays(AggregateFunction):
    """Keep only an integer accumulator for each user/window."""

    def create_accumulator(self):
        return 0

    def add(self, value, accumulator):
        return accumulator + int(json.loads(value)["event_type"] == "play")

    def get_result(self, accumulator):
        return accumulator

    def merge(self, left, right):
        return left + right


class EmitFeature(ProcessWindowFunction):
    """Attach entity and reference-compatible window metadata."""

    def process(self, key, context, elements):
        window = context.window()

        yield json.dumps(
            {
                "feature_schema_version": "v1",
                "feature_name": "user_play_count_1h",
                "user_id": key,
                "observation_time": utc_timestamp(window.end),
                "window_start_exclusive": utc_timestamp(window.start),
                "play_count": next(iter(elements)),
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def late_event_record(payload: str) -> str:
    return json.dumps(
        {
            "error_schema_version": "v1",
            "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
            "feature_name": "user_play_count_1h",
            "raw_payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("featureforge-stream-user-play-count-1h-v1")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_events = env.from_source(
        source,
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
        .uid("user-play-count-1h-deduplicate-v1")
    )

    features = (
        unique_events.key_by(user_id, key_type=Types.STRING())
        .window(
            SlidingEventTimeWindows.of(
                Time.hours(1),
                Time.minutes(1),
            )
        )
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .side_output_late_data(LATE_EVENTS)
        .aggregate(
            CountPlays(),
            EmitFeature(),
            accumulator_type=Types.LONG(),
            output_type=Types.STRING(),
        )
        .name("user-play-count-1h-windows")
        .uid("user-play-count-1h-windows-v1")
    )

    features.sink_to(kafka_sink(FEATURE_TOPIC)).name("user-play-count-1h-kafka-sink")

    features.get_side_output(LATE_EVENTS).map(
        late_event_record,
        output_type=Types.STRING(),
    ).sink_to(kafka_sink(LATE_TOPIC)).name("late-events-kafka-sink")

    env.execute("featureforge-stream-user-play-count-1h")


if __name__ == "__main__":
    main()
