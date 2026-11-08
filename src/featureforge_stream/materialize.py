"""Materialize user_play_count_1h Kafka updates into Redis."""

import argparse
import json
import logging
from collections import Counter

from confluent_kafka import Consumer, KafkaException, TopicPartition
from redis import Redis

from featureforge_stream.online_store import (
    RedisUserPlayCountStore,
    UserPlayCountFeatureV1,
)

LOGGER = logging.getLogger(__name__)

DEFAULT_TOPIC = "features.user-play-count-1h.v1"
DEFAULT_KEY_PREFIX = "featureforge-stream:online:v1:user_play_count_1h"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize user_play_count_1h Kafka updates into Redis."
    )
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--key-prefix", default=DEFAULT_KEY_PREFIX)
    parser.add_argument("--commit-every", type=int, default=1000)
    parser.add_argument("--bounded", action="store_true")
    return parser.parse_args()


def commit_offset(
    consumer: Consumer,
    partition: TopicPartition,
    next_offset: int,
) -> None:
    result = consumer.commit(
        offsets=[
            TopicPartition(
                partition.topic,
                partition.partition,
                next_offset,
            )
        ],
        asynchronous=False,
    )

    for committed in result or []:
        if committed.error is not None:
            raise KafkaException(committed.error)


def materialize(arguments: argparse.Namespace) -> Counter:
    if arguments.commit_every < 1:
        raise ValueError("--commit-every must be positive")

    results: Counter = Counter()

    with Redis.from_url(
        arguments.redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        if redis_client.ping() is not True:
            raise RuntimeError("Redis connectivity check failed")

        store = RedisUserPlayCountStore(
            redis_client,
            key_prefix=arguments.key_prefix,
        )

        consumer = Consumer(
            {
                "bootstrap.servers": arguments.bootstrap_servers,
                "broker.address.family": "v4",
                "group.id": arguments.group_id,
                "client.id": "featureforge-stream-redis-materializer",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "allow.auto.create.topics": False,
            }
        )

        try:
            metadata = consumer.list_topics(arguments.topic, timeout=10)
            topic_metadata = metadata.topics.get(arguments.topic)

            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Topic is not available: {arguments.topic}")

            if len(topic_metadata.partitions) != 1:
                raise RuntimeError("Only single-partition feature topics are supported")

            partition_id = next(iter(topic_metadata.partitions))
            partition = TopicPartition(arguments.topic, partition_id)

            low, high = consumer.get_watermark_offsets(
                partition,
                timeout=10,
                cached=False,
            )

            if low == high:
                raise RuntimeError(
                    f"No readable records in topic {arguments.topic!r}: "
                    f"earliest_available_offset={low}, end_offset={high}. "
                    "Refusing to materialize an empty Kafka range."
                )

            committed = consumer.committed([partition], timeout=10)[0]
            if committed.error is not None:
                raise KafkaException(committed.error)

            start = committed.offset if committed.offset >= 0 else low
            start = max(start, low)

            LOGGER.info(
                "Start: topic=%s group=%s start_offset=%s end_offset=%s bounded=%s",
                arguments.topic,
                arguments.group_id,
                start,
                high,
                arguments.bounded,
            )

            if arguments.bounded and start >= high:
                LOGGER.info(
                    "No pending records for group=%s at end_offset=%s",
                    arguments.group_id,
                    high,
                )
                return results

            consumer.assign(
                [
                    TopicPartition(
                        arguments.topic,
                        partition_id,
                        start,
                    )
                ]
            )

            since_commit = 0
            last_offset = None

            while True:
                message = consumer.poll(timeout=1.0)

                if message is None:
                    continue

                if message.error():
                    raise KafkaException(message.error())

                try:
                    payload = message.value()
                    if payload is None:
                        raise ValueError("Feature tombstones are not supported")

                    feature = UserPlayCountFeatureV1.model_validate_json(payload)
                    results[store.write(feature)] += 1
                except Exception:
                    LOGGER.exception(
                        "Materialization failed at partition=%s offset=%s",
                        message.partition(),
                        message.offset(),
                    )
                    raise

                last_offset = message.offset()
                since_commit += 1

                if since_commit >= arguments.commit_every:
                    commit_offset(consumer, partition, last_offset + 1)
                    since_commit = 0

                if arguments.bounded and last_offset + 1 >= high:
                    break

            if since_commit:
                commit_offset(consumer, partition, last_offset + 1)

            return results
        finally:
            consumer.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    results = materialize(parse_arguments())
    print(
        json.dumps(
            {"store_results": dict(sorted(results.items()))},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
