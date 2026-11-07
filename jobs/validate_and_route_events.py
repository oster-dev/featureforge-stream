"""Validate Kafka events in continuous or bounded replay mode."""

import argparse

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.output_tag import OutputTag

from featureforge_stream.contracts.routing import route_event_json

BOOTSTRAP_SERVERS = "kafka:29092"
INPUT_TOPIC = "interaction-events.v1"
VALIDATED_TOPIC = "interaction-events.validated.v1"
REJECTED_TOPIC = "interaction-events.rejected.v1"

INVALID_EVENTS = OutputTag("invalid-events-v1", Types.STRING())


class ValidateAndRoute(ProcessFunction):
    """Keep validation failures out of the valid event stream."""

    def process_element(self, value, ctx):
        valid, rejected = route_event_json(value)

        if valid is not None:
            yield valid
        elif rejected is not None:
            yield INVALID_EVENTS, rejected


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
        description="Validate interaction events and route rejected records."
    )
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--input-topic", default=INPUT_TOPIC)
    parser.add_argument("--validated-topic", default=VALIDATED_TOPIC)
    parser.add_argument("--rejected-topic", default=REJECTED_TOPIC)
    parser.add_argument(
        "--group-id",
        default="featureforge-stream-validation-v1",
    )
    parser.add_argument(
        "--job-name",
        default="featureforge-stream-validation",
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

    stream = env.from_source(
        source_builder.build(),
        WatermarkStrategy.no_watermarks(),
        "interaction-events-source",
    )

    valid = stream.process(
        ValidateAndRoute(),
        output_type=Types.STRING(),
    ).name("validate-and-route-interaction-event-v1")

    valid.sink_to(
        kafka_sink(
            arguments.validated_topic,
            bootstrap_servers=arguments.bootstrap_servers,
        )
    ).name("validated-events-sink")

    valid.get_side_output(INVALID_EVENTS).sink_to(
        kafka_sink(
            arguments.rejected_topic,
            bootstrap_servers=arguments.bootstrap_servers,
        )
    ).name("rejected-events-sink")

    env.execute(arguments.job_name)


if __name__ == "__main__":
    main()
