"""Compare a declared Kafka event input with a verified replay snapshot."""

import argparse
import hashlib
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, TopicPartition

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.reference.snapshot_parity import compare_snapshot_with_coverage
from featureforge_stream.snapshot import read_snapshot


def read_events(
    consumer: Consumer,
    *,
    topic: str,
    timeout_seconds: float,
) -> tuple[list[InteractionEventV1], dict]:
    """Capture a complete, contiguous single-partition event input."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be finite and positive")

    metadata = consumer.list_topics(topic, timeout=10)
    info = metadata.topics.get(topic)
    if info is None or info.error is not None:
        raise RuntimeError(f"Input topic is unavailable: {topic}")
    if len(info.partitions) != 1:
        raise RuntimeError("Comparison requires exactly one input partition")

    partition_id = next(iter(info.partitions))
    partition = TopicPartition(topic, partition_id)
    low, high = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
    if low != 0 or high <= low:
        raise RuntimeError(
            f"Expected complete non-empty input from offset zero: low={low}, high={high}"
        )

    consumer.assign([TopicPartition(topic, partition_id, low)])
    events = []
    digest = hashlib.sha256()
    next_offset = low
    deadline = time.monotonic() + timeout_seconds

    while next_offset < high:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Input read timed out at offset {next_offset}; target={high}")

        message = consumer.poll(min(1.0, remaining))
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())
        if (
            message.topic() != topic
            or message.partition() != partition_id
            or message.offset() != next_offset
        ):
            raise RuntimeError(f"Unexpected input position at offset {next_offset}")

        payload = message.value()
        if payload is None:
            raise ValueError(f"Input tombstone at offset {next_offset}")

        event = InteractionEventV1.model_validate_json(payload)
        events.append(event)
        normalized = json.dumps(
            event.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        digest.update(normalized.encode("utf-8") + b"\n")
        next_offset += 1

    final_range = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
    if final_range != (low, high):
        raise RuntimeError("Input range changed during comparison")

    return events, {
        "topic": topic,
        "partition": partition_id,
        "start_offset_inclusive": low,
        "end_offset_exclusive": high,
        "records_read": len(events),
        "normalized_events_sha256": digest.hexdigest(),
        "hash_encoding": "sorted-key compact UTF-8 event JSON plus LF per delivery",
    }


def load_verified_snapshot(directory: Path) -> tuple[list, dict]:
    """Validate snapshot bytes against the successful export report."""
    export = json.loads((directory / "verification.json").read_text())
    if export["export_contract"] != "featureforge-stream.replay-snapshot-export.v1":
        raise ValueError("Unsupported export report contract")
    if export["comparison"]["passed"] is not True:
        raise ValueError("Original export verification did not pass")
    if export["snapshot"]["file"] != "windows.parquet":
        raise ValueError("Unsupported snapshot filename")

    path = directory / "windows.parquet"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != export["snapshot"]["sha256"]:
        raise ValueError("Snapshot SHA-256 does not match the export report")

    windows = read_snapshot(path)
    if (
        len(windows) != export["snapshot"]["window_rows"]
        or len(windows) != export["snapshot"]["restored_rows"]
    ):
        raise ValueError("Snapshot row counts do not match the export report")

    return windows, {
        "file": path.name,
        "sha256": digest,
        "feature_output_source": export["source"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare source events with snapshot counts and complete window coverage."
    )
    parser.add_argument("--input-topic", required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--timeout-seconds", type=float, default=120)
    arguments = parser.parse_args()

    if not arguments.input_topic.strip():
        parser.error("--input-topic must not be blank")
    if not math.isfinite(arguments.timeout_seconds) or arguments.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be finite and positive")
    if arguments.report.exists():
        parser.error("--report must not already exist")

    windows, snapshot = load_verified_snapshot(arguments.snapshot_dir)

    consumer = Consumer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "broker.address.family": "v4",
            "group.id": f"source-snapshot-check-{uuid4().hex}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "error",
        }
    )
    try:
        events, source = read_events(
            consumer,
            topic=arguments.input_topic,
            timeout_seconds=arguments.timeout_seconds,
        )
    finally:
        consumer.close()

    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "input": source,
        "snapshot": snapshot,
        "lineage_boundary": (
            "Input-to-feature-output association is declared by the caller, "
            "not independently proven by this checker. Original Parquet adaptation "
            "is outside this comparison."
        ),
        "comparison": compare_snapshot_with_coverage(events, windows),
    }

    arguments.report.parent.mkdir(parents=True, exist_ok=True)
    with arguments.report.open("x") as target:
        json.dump(report, target, indent=2)
        target.write("\n")

    print(json.dumps(report, indent=2))
    print(f"\nReport written: {arguments.report}")
    if not report["comparison"]["passed"]:
        raise SystemExit(
            "MISMATCH: counts or window coverage differ from full event-time reconstruction"
        )
    print("PASS: snapshot counts and complete window coverage match source reconstruction")


if __name__ == "__main__":
    main()
