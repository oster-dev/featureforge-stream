"""Compare the running Flink feature job with the golden reference."""

import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4, uuid5

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.reference.user_play_count import count_user_plays_1h

FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "user_play_count_1h_v1.jsonl"

BOOTSTRAP_SERVERS = "kafka:29092"
INPUT_TOPIC = "interaction-events.v1"
FEATURE_TOPIC = "features.user-play-count-1h.v1"

FIXTURE_OBSERVATION_TIME = datetime(2026, 1, 1, 12, tzinfo=UTC)


def run_kafka(
    script: str,
    *arguments: str,
    input_text: str | None = None,
) -> str:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            f"/opt/kafka/bin/{script}",
            *arguments,
        ],
        input=input_text,
        check=True,
        capture_output=True,
        text=True,
        timeout=45,
    )
    return result.stdout


def produce_events(events: list[InteractionEventV1]) -> None:
    run_kafka(
        "kafka-console-producer.sh",
        "--bootstrap-server",
        BOOTSTRAP_SERVERS,
        "--topic",
        INPUT_TOPIC,
        input_text="\n".join(event.model_dump_json() for event in events) + "\n",
    )


def read_features() -> list[dict]:
    output = run_kafka(
        "kafka-console-consumer.sh",
        "--bootstrap-server",
        BOOTSTRAP_SERVERS,
        "--topic",
        FEATURE_TOPIC,
        "--from-beginning",
        "--timeout-ms",
        "3000",
    )

    return [json.loads(line) for line in output.splitlines() if line.strip()]


def wait_for_features(
    *,
    observation_time: datetime,
    expected: dict[str, int],
    timeout_seconds: int = 60,
) -> dict[str, dict]:
    deadline = time.monotonic() + timeout_seconds
    found: dict[str, dict] = {}

    while time.monotonic() < deadline:
        for record in read_features():
            entity = record.get("user_id")
            if entity not in expected:
                continue

            record_time = datetime.fromisoformat(record["observation_time"])
            if record_time == observation_time:
                found[entity] = record

        if all(
            entity in found and found[entity].get("play_count") == expected_count
            for entity, expected_count in expected.items()
        ):
            return found

        time.sleep(0.5)

    actual = {entity: record.get("play_count") for entity, record in found.items()}
    raise AssertionError(
        f"Feature values not observed at {observation_time.isoformat()}: "
        f"expected={expected}, actual={actual}"
    )


@pytest.mark.integration
def test_golden_fixture_matches_flink_feature_values() -> None:
    fixture_events = [
        InteractionEventV1.model_validate_json(line)
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    namespace = uuid4()
    observation_time = datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(minutes=2)
    shift = observation_time - FIXTURE_OBSERVATION_TIME

    user_mapping = {
        original_user: f"integration_{namespace.hex}_{index}"
        for index, original_user in enumerate(
            sorted({event.user_id for event in fixture_events}),
            start=1,
        )
    }

    events = [
        InteractionEventV1.model_validate(
            {
                **event.model_dump(),
                "event_id": uuid5(namespace, str(event.event_id)),
                "user_id": user_mapping[event.user_id],
                "event_time": event.event_time + shift,
                "ingested_at": event.ingested_at + shift,
            }
        )
        for event in fixture_events
    ]

    expected = {
        mapped_user: count_user_plays_1h(
            events,
            user_id=mapped_user,
            observation_time=observation_time,
        )
        for mapped_user in user_mapping.values()
    }

    assert expected[user_mapping["user_000001"]] == 3
    assert expected[user_mapping["user_000002"]] == 1

    # Keep arrival order chronological to isolate counting and deduplication.
    ordered_events = sorted(events, key=lambda event: event.event_time)

    # Advance the watermark beyond the observation time.
    progress_time = observation_time + timedelta(minutes=11)
    progress_event = InteractionEventV1.model_validate(
        {
            **fixture_events[0].model_dump(),
            "event_id": uuid4(),
            "user_id": f"integration_progress_{namespace.hex}",
            "event_time": progress_time,
            "ingested_at": progress_time,
            "event_type": "skip",
            "play_seconds": 0,
        }
    )

    produce_events([*ordered_events, progress_event])

    actual = wait_for_features(
        observation_time=observation_time,
        expected=expected,
    )

    for entity, expected_count in expected.items():
        feature = actual[entity]

        assert feature["feature_schema_version"] == "v1"
        assert feature["feature_name"] == "user_play_count_1h"
        assert feature["play_count"] == expected_count

        window_start = datetime.fromisoformat(feature["window_start_exclusive"])
        assert window_start == observation_time - timedelta(hours=1)
