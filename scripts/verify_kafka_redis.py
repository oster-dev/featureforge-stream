"""Compare retained Kafka feature output with an isolated Redis namespace."""

import argparse
import json
import math
import time
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, TopicPartition
from redis import Redis

from featureforge_stream.online_store import (
    RedisUserPlayCountStore,
    UserPlayCountFeatureV1,
)


def select_latest(
    expected: dict[str, UserPlayCountFeatureV1],
    feature: UserPlayCountFeatureV1,
) -> None:
    """Select the latest window, then the highest count within that window."""
    previous = expected.get(feature.user_id)
    if previous is None or (
        feature.observation_time,
        feature.play_count,
    ) > (
        previous.observation_time,
        previous.play_count,
    ):
        expected[feature.user_id] = feature


def read_expected(
    consumer: Consumer,
    *,
    topic: str,
    timeout_seconds: float,
) -> tuple[dict[str, UserPlayCountFeatureV1], int, int]:
    """Read one complete, contiguous partition without committing offsets."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be finite and positive")

    metadata = consumer.list_topics(topic, timeout=10)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"Topic is not available: {topic}")

    if len(topic_metadata.partitions) != 1:
        raise RuntimeError("Verification supports exactly one topic partition")

    partition_id = next(iter(topic_metadata.partitions))
    partition = TopicPartition(topic, partition_id)
    low, high = consumer.get_watermark_offsets(partition, timeout=10, cached=False)

    if low != 0 or high <= low:
        raise RuntimeError(
            f"Expected a complete non-empty range from offset zero: low={low}, high={high}"
        )

    consumer.assign([TopicPartition(topic, partition_id, low)])
    expected: dict[str, UserPlayCountFeatureV1] = {}
    next_offset = low
    deadline = time.monotonic() + timeout_seconds

    while next_offset < high:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Kafka verification timed out at offset {next_offset}; target={high}"
            )

        message = consumer.poll(timeout=min(1.0, remaining))
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())

        if (
            message.topic() != topic
            or message.partition() != partition_id
            or message.offset() != next_offset
        ):
            raise RuntimeError(
                f"Unexpected Kafka position: topic={message.topic()}, "
                f"partition={message.partition()}, offset={message.offset()}, "
                f"expected_offset={next_offset}"
            )

        payload = message.value()
        if payload is None:
            raise ValueError(f"Tombstone at offset {next_offset} is not supported")

        feature = UserPlayCountFeatureV1.model_validate_json(payload)
        select_latest(expected, feature)
        next_offset += 1

    final_range = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
    if final_range != (low, high):
        raise RuntimeError(
            f"Kafka range changed during verification: initial={(low, high)}, final={final_range}"
        )

    return expected, next_offset - low, high


def compare_redis(
    client: Redis,
    *,
    key_prefix: str,
    expected: dict[str, UserPlayCountFeatureV1],
) -> dict:
    """Compare all expected values and identify additional namespace keys."""
    if not key_prefix.strip():
        raise ValueError("key_prefix must not be blank")
    if not expected:
        raise ValueError("Refusing to verify an empty expected feature set")

    store = RedisUserPlayCountStore(client, key_prefix=key_prefix)
    mismatches = []

    for user_id, feature in sorted(expected.items()):
        actual = store.read(user_id)
        if actual != feature:
            mismatches.append(
                {
                    "user_id": user_id,
                    "expected": feature.model_dump(mode="json"),
                    "actual": actual.model_dump(mode="json") if actual else None,
                }
            )

    namespace = f"{key_prefix}:"
    actual_keys = {key for key in client.scan_iter(count=1000) if key.startswith(namespace)}
    expected_keys = {store.key_for(user_id) for user_id in expected}
    extra_keys = sorted(actual_keys - expected_keys)

    return {
        "expected_users": len(expected),
        "redis_keys": len(actual_keys),
        "mismatch_count": len(mismatches),
        "extra_key_count": len(extra_keys),
        "mismatch_examples": mismatches[:3],
        "extra_key_examples": extra_keys[:3],
        "passed": not mismatches and not extra_keys,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify latest Kafka feature values against Redis."
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument("--key-prefix", required=True)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--timeout-seconds", type=float, default=180)
    arguments = parser.parse_args()

    if not arguments.topic.strip() or not arguments.key_prefix.strip():
        parser.error("--topic and --key-prefix must not be blank")
    if not math.isfinite(arguments.timeout_seconds) or arguments.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be finite and positive")

    consumer = Consumer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "broker.address.family": "v4",
            "group.id": f"verify-online-{uuid4().hex}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "error",
        }
    )

    try:
        expected, records, end_offset = read_expected(
            consumer,
            topic=arguments.topic,
            timeout_seconds=arguments.timeout_seconds,
        )
    finally:
        consumer.close()

    with Redis.from_url(
        arguments.redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as client:
        comparison = compare_redis(
            client,
            key_prefix=arguments.key_prefix,
            expected=expected,
        )

    report = {
        "topic": arguments.topic,
        "key_prefix": arguments.key_prefix,
        "kafka_records_checked": records,
        "captured_end_offset": end_offset,
        **comparison,
    }
    print(json.dumps(report, indent=2))

    if not comparison["passed"]:
        raise SystemExit("FAIL: Kafka output and Redis differ")

    print("PASS: latest Kafka feature values match Redis")


if __name__ == "__main__":
    main()
