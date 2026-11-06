"""Validate Kafka events and write valid and rejected records separately."""

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
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

    # Checkpointing is required for reliable source-offset recovery and for
    # Kafka sink delivery guarantees.
    env.enable_checkpointing(10_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("featureforge-stream-validation-v1")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "interaction-events-source",
    )

    valid = stream.process(
        ValidateAndRoute(),
        output_type=Types.STRING(),
    ).name("validate-and-route-interaction-event-v1")

    valid.sink_to(kafka_sink(VALIDATED_TOPIC)).name("validated-events-sink")
    valid.get_side_output(INVALID_EVENTS).sink_to(kafka_sink(REJECTED_TOPIC)).name(
        "rejected-events-sink"
    )

    env.execute("featureforge-stream-validation")


if __name__ == "__main__":
    main()
