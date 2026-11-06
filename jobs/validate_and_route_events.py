"""Route valid and invalid Kafka event JSON into separate outputs."""

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream.output_tag import OutputTag

from featureforge_stream.contracts.routing import route_event_json

INVALID_EVENTS = OutputTag("invalid-events-v1", Types.STRING())


class ValidateAndRoute(ProcessFunction):
    """Keep validation failures out of the valid event stream."""

    def process_element(self, value, ctx):
        valid, rejected = route_event_json(value)

        if valid is not None:
            yield valid
        elif rejected is not None:
            yield INVALID_EVENTS, rejected


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:29092")
        .set_topics("interaction-events.v1")
        .set_group_id("featureforge-stream-routing-smoke-v1")
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

    valid.print("valid-event")
    valid.get_side_output(INVALID_EVENTS).print("rejected-event")

    env.execute("featureforge-stream-routing-smoke")


if __name__ == "__main__":
    main()
