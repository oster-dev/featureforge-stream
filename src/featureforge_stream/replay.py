"""Adapt and replay FeatureForge Parquet events into Kafka."""

import argparse
import json
from collections import Counter
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pyarrow.parquet as pq

from featureforge_stream.contracts.events import InteractionEventV1

SUPPORTED_EVENT_TYPES = {"play", "impression"}

DEVICE_SOURCES = {
    "web": "web",
    "android": "mobile",
    "ios": "mobile",
    "mobile": "mobile",
    "tv": "tv",
}


def load_events(
    path: Path,
    *,
    dataset_id: str,
) -> tuple[list[InteractionEventV1], dict]:
    """Validate supported deliveries without changing source timestamps."""
    if not dataset_id.strip():
        raise ValueError("dataset_id must not be blank")

    table = pq.read_table(path)
    required_columns = {
        "event_id",
        "user_id",
        "content_id",
        "event_type",
        "event_time",
        "ingested_at",
        "device_type",
        "watch_seconds",
    }

    missing = required_columns - set(table.column_names)
    if missing:
        raise ValueError(f"Missing source columns: {sorted(missing)}")

    namespace = uuid5(
        NAMESPACE_URL,
        f"featureforge-stream:source-dataset:{dataset_id}",
    )

    source_counts = Counter()
    skipped_counts = Counter()
    events = []
    logical_payloads = {}

    for row_number, row in enumerate(table.to_pylist(), start=1):
        event_type = row["event_type"]
        source_counts[event_type] += 1

        if event_type not in SUPPORTED_EVENT_TYPES:
            skipped_counts[event_type] += 1
            continue

        try:
            source_id = row["event_id"]
            if not isinstance(source_id, str) or not source_id.strip():
                raise ValueError("source event_id must be a non-empty string")

            device_type = row["device_type"]
            if device_type not in DEVICE_SOURCES:
                raise ValueError(f"Unsupported device_type: {device_type!r}")

            event = InteractionEventV1.model_validate(
                {
                    "event_id": uuid5(namespace, source_id),
                    "event_time": row["event_time"],
                    "ingested_at": row["ingested_at"],
                    "user_id": row["user_id"],
                    "content_id": row["content_id"],
                    "event_type": event_type,
                    "play_seconds": row["watch_seconds"],
                    "source": DEVICE_SOURCES[device_type],
                    "schema_version": "v1",
                }
            )

            logical_payload = event.model_dump_json(exclude={"ingested_at"})
            previous = logical_payloads.get(event.event_id)

            if previous is not None and previous != logical_payload:
                raise ValueError(f"Conflicting logical payload for source event_id={source_id!r}")

            logical_payloads[event.event_id] = logical_payload
            events.append(event)
        except ValueError as exc:
            raise ValueError(f"Cannot adapt source row {row_number}: {exc}") from exc

    if not events:
        raise ValueError("No supported events found in the source file")

    # Stable sorting preserves original file order for equal ingestion times.
    events.sort(key=lambda event: event.ingested_at)

    report = {
        "source_file": str(path.resolve()),
        "dataset_id": dataset_id,
        "source_records": table.num_rows,
        "source_event_types": dict(sorted(source_counts.items())),
        "adapted_records": len(events),
        "adapted_event_types": dict(sorted(Counter(event.event_type for event in events).items())),
        "skipped_event_types": dict(sorted(skipped_counts.items())),
        "unique_adapted_event_ids": len(logical_payloads),
        "duplicate_deliveries": len(events) - len(logical_payloads),
        "event_time_min": min(event.event_time for event in events).isoformat(),
        "event_time_max": max(event.event_time for event in events).isoformat(),
        "ingested_at_min": events[0].ingested_at.isoformat(),
        "ingested_at_max": events[-1].ingested_at.isoformat(),
        "arrival_order": "ingested_at, then original file order",
        "time_policy": "preserve source timestamps",
        "delivery_mode": "accelerated replay; no wall-clock pacing",
    }

    return events, report


def publish_events(
    events: list[InteractionEventV1],
    *,
    bootstrap_servers: str,
    topic: str,
) -> int:
    """Publish to partition zero of a dedicated replay topic."""
    from confluent_kafka import Producer

    if not topic.startswith("interaction-events.replay."):
        raise ValueError(
            "Historical replay requires a dedicated topic named "
            "'interaction-events.replay.<run-name>.v1'. "
            "The existing test input topic is intentionally protected."
        )

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "featureforge-stream-featureforge-replay",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 30000,
            "allow.auto.create.topics": False,
        }
    )

    errors = []
    delivered = 0

    def on_delivery(error, message):
        nonlocal delivered

        if error is not None:
            errors.append(str(error))
        else:
            delivered += 1

    for event in events:
        while True:
            if errors:
                raise RuntimeError(f"Kafka delivery failed: {errors[0]}")

            try:
                producer.produce(
                    topic=topic,
                    partition=0,
                    key=event.user_id,
                    value=event.model_dump_json(),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(1.0)

        producer.poll(0)

    pending = producer.flush(60)

    if errors or pending or delivered != len(events):
        raise RuntimeError(
            "Kafka replay was not fully delivered: "
            f"expected={len(events)}, delivered={delivered}, "
            f"pending={pending}, errors={errors[:3]}"
        )

    return delivered


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Adapt FeatureForge Parquet events for streaming replay."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--topic")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    arguments = parser.parse_args()

    if arguments.send and not arguments.topic:
        parser.error("--send requires --topic")

    events, report = load_events(
        arguments.input,
        dataset_id=arguments.dataset_id,
    )

    print(json.dumps(report, indent=2))
    print("\n--- First 3 adapted deliveries ---")
    for event in events[:3]:
        print(event.model_dump_json())

    if not arguments.send:
        print("\nPREVIEW ONLY: no Kafka connection or writes.")
        return

    delivered = publish_events(
        events,
        bootstrap_servers=arguments.bootstrap_servers,
        topic=arguments.topic,
    )

    print(
        json.dumps(
            {
                "status": "delivered",
                "topic": arguments.topic,
                "delivered_records": delivered,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
