"""Validate Kafka event JSON in a Python operator on the TaskManager."""

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)

from featureforge_stream.contracts.events import validate_event_json


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("interaction-events.v1")
        .set_group_id("featureforge-stream-validation-smoke-v1")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "interaction-events-source",
    )

    validated = stream.map(
        validate_event_json,
        output_type=Types.STRING(),
    ).name("validate-interaction-event-v1")

    validated.print("validated-event")

    env.execute("featureforge-stream-validation-smoke")


if __name__ == "__main__":
    main()
