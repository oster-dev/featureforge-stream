"""Explain source/snapshot differences using verified observed late output."""

import argparse
import hashlib
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, TopicPartition
from verify_source_snapshot import load_verified_snapshot, read_events

from featureforge_stream.reference.reconciliation import reconcile_late_output


def read_late_records(
    consumer: Consumer,
    *,
    topic: str,
    timeout_seconds: float,
) -> tuple[list[dict], dict]:
    """Read a complete single-partition late output; an empty range is allowed."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be finite and positive")

    metadata = consumer.list_topics(topic, timeout=10)
    info = metadata.topics.get(topic)
    if info is None or info.error is not None:
        raise RuntimeError(f"Late topic is unavailable: {topic}")
    if len(info.partitions) != 1:
        raise RuntimeError("Expected exactly one late-topic partition")

    partition_id = next(iter(info.partitions))
    partition = TopicPartition(topic, partition_id)
    low, high = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
    if low != 0 or high < low:
        raise RuntimeError(f"Unsupported late-output range: low={low}, high={high}")

    consumer.assign([TopicPartition(topic, partition_id, low)])
    records = []
    digest = hashlib.sha256()
    next_offset = low
    deadline = time.monotonic() + timeout_seconds

    while next_offset < high:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Late-output read timed out at offset {next_offset}")

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
            raise RuntimeError("Unexpected late-output record position")

        payload = message.value()
        if payload is None:
            raise ValueError("Late-output tombstones are unsupported")

        record = json.loads(payload)
        if not isinstance(record, dict):
            raise TypeError("Late-output records must be JSON objects")

        records.append(record)
        digest.update(
            json.dumps(
                record,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
            + b"\n"
        )
        next_offset += 1

    if consumer.get_watermark_offsets(partition, timeout=10, cached=False) != (low, high):
        raise RuntimeError("Late-output range changed during inspection")

    return records, {
        "topic": topic,
        "partition": partition_id,
        "start_offset_inclusive": low,
        "end_offset_exclusive": high,
        "records_read": len(records),
        "normalized_envelopes_sha256": digest.hexdigest(),
        "hash_encoding": "sorted-key compact ASCII-escaped JSON plus LF per envelope",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile snapshot reconstruction with observed late-output exclusions."
    )
    parser.add_argument("--input-topic", required=True)
    parser.add_argument("--late-topic", required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--timeout-seconds", type=float, default=120)
    arguments = parser.parse_args()

    if not arguments.input_topic.strip() or not arguments.late_topic.strip():
        parser.error("topic names must not be blank")
    if not math.isfinite(arguments.timeout_seconds) or arguments.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be finite and positive")
    if arguments.report.exists():
        parser.error("--report must not already exist")

    windows, snapshot = load_verified_snapshot(arguments.snapshot_dir)

    consumer = Consumer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "broker.address.family": "v4",
            "group.id": f"late-reconciliation-{uuid4().hex}",
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
        late_records, late_source = read_late_records(
            consumer,
            topic=arguments.late_topic,
            timeout_seconds=arguments.timeout_seconds,
        )
    finally:
        consumer.close()

    reconciliation = reconcile_late_output(events, windows, late_records)
    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "input": source,
        "late_output": late_source,
        "snapshot": snapshot,
        "lineage_boundary": (
            "The caller declares that input, late output, and feature output belong "
            "to the same completed run. Topic association is not independently proven."
        ),
        "reconciliation": reconciliation,
    }

    arguments.report.parent.mkdir(parents=True, exist_ok=True)
    with arguments.report.open("x") as target:
        json.dump(report, target, indent=2)
        target.write("\n")

    print(json.dumps(report, indent=2))
    print(f"\nReport written: {arguments.report}")

    if not reconciliation["consistent_with_observed_exclusions"]:
        raise SystemExit("UNEXPLAINED: differences remain after observed exclusions")

    print(
        "RECONCILED: snapshot matches reconstruction after verified observed exclusions; "
        "watermark decisions are not independently verified"
    )


if __name__ == "__main__":
    main()
