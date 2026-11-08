"""Export bounded replay output and verify persisted snapshot values against Redis."""

import argparse
import hashlib
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, TopicPartition
from redis import Redis
from verify_kafka_redis import compare_redis, select_latest

from featureforge_stream.online_store import UserPlayCountFeatureV1
from featureforge_stream.snapshot import read_snapshot, write_snapshot


def read_windows(
    consumer: Consumer,
    *,
    topic: str,
    timeout_seconds: float,
) -> tuple[list[UserPlayCountFeatureV1], dict]:
    """Read one complete contiguous partition, retaining every delivered window."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be finite and positive")

    metadata = consumer.list_topics(topic, timeout=10)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"Topic is not available: {topic}")
    if len(topic_metadata.partitions) != 1:
        raise RuntimeError("Export supports exactly one topic partition")

    partition_id = next(iter(topic_metadata.partitions))
    partition = TopicPartition(topic, partition_id)
    low, high = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
    if low != 0 or high <= low:
        raise RuntimeError(
            f"Expected a complete non-empty range from offset zero: low={low}, high={high}"
        )

    consumer.assign([TopicPartition(topic, partition_id, low)])
    features = []
    next_offset = low
    deadline = time.monotonic() + timeout_seconds

    while next_offset < high:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Snapshot export timed out at offset {next_offset}; target={high}")

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
            raise RuntimeError(f"Unexpected Kafka position at expected offset {next_offset}")

        payload = message.value()
        if payload is None:
            raise ValueError(f"Tombstone at offset {next_offset} is not supported")

        features.append(UserPlayCountFeatureV1.model_validate_json(payload))
        next_offset += 1

    final_range = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
    if final_range != (low, high):
        raise RuntimeError(
            f"Kafka range changed during export: initial={(low, high)}, final={final_range}"
        )

    return features, {
        "topic": topic,
        "partition": partition_id,
        "start_offset_inclusive": low,
        "end_offset_exclusive": high,
        "records_read": len(features),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export replay windows to Parquet and compare latest values with Redis."
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument("--key-prefix", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--timeout-seconds", type=float, default=180)
    arguments = parser.parse_args()

    if not arguments.topic.strip() or not arguments.key_prefix.strip():
        parser.error("--topic and --key-prefix must not be blank")
    if not math.isfinite(arguments.timeout_seconds) or arguments.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be finite and positive")
    if arguments.output_dir.exists():
        parser.error("--output-dir must not already exist")

    consumer = Consumer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "broker.address.family": "v4",
            "group.id": f"snapshot-export-{uuid4().hex}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "error",
        }
    )
    started_at = datetime.now(UTC)

    try:
        features, source = read_windows(
            consumer,
            topic=arguments.topic,
            timeout_seconds=arguments.timeout_seconds,
        )
    finally:
        consumer.close()

    arguments.output_dir.mkdir(parents=True, exist_ok=False)
    snapshot_path = arguments.output_dir / "windows.parquet"
    rows = write_snapshot(features, output_path=snapshot_path)
    del features

    restored = read_snapshot(snapshot_path)
    expected: dict[str, UserPlayCountFeatureV1] = {}
    for feature in restored:
        select_latest(expected, feature)

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
        "export_contract": "featureforge-stream.replay-snapshot-export.v1",
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "source": source,
        "snapshot": {
            "file": snapshot_path.name,
            "sha256": hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
            "window_rows": rows,
            "restored_rows": len(restored),
        },
        "redis_key_prefix": arguments.key_prefix,
        "comparison": comparison,
        "scope": "quiescent replay snapshot consistency, not batch-stream parity",
    }

    report_path = arguments.output_dir / "verification.json"
    with report_path.open("x") as target:
        json.dump(report, target, indent=2)
        target.write("\n")

    print(json.dumps(report, indent=2))
    if not comparison["passed"]:
        raise SystemExit("FAIL: persisted snapshot and Redis differ")

    print("PASS: persisted snapshot latest values match Redis")


if __name__ == "__main__":
    main()
