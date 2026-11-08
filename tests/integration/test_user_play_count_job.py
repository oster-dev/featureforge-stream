"""Verify golden values, late corrections, duplicates, and too-late routing."""

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


def assert_no_excess_play_count(
    *,
    user_id: str,
    observation_time: datetime,
    expected_count: int,
    duration_seconds: int = 10,
) -> None:
    """Reject any excessive count observed during a bounded polling period."""
    deadline = time.monotonic() + duration_seconds

    while time.monotonic() < deadline:
        for record in read_features():
            if record.get("user_id") != user_id:
                continue

            record_time = datetime.fromisoformat(record["observation_time"])
            if record_time != observation_time:
                continue

            assert record["play_count"] <= expected_count, (
                "Duplicate delivery increased the feature value: "
                f"expected at most {expected_count}, record={record}"
            )

        time.sleep(0.5)


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


@pytest.mark.integration
def test_late_event_corrects_previously_emitted_feature() -> None:
    """Correct an emitted window and ignore a repeated late-event delivery."""
    namespace = uuid4()
    user_id = f"integration_late_{namespace.hex}"

    # Stay ahead of the golden test's progress event on the shared job.
    observation_time = datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(minutes=15)
    initial_time = observation_time - timedelta(minutes=1)
    late_time = observation_time - timedelta(seconds=30)
    progress_time = observation_time + timedelta(minutes=11)

    initial_event = InteractionEventV1.model_validate(
        {
            "event_id": uuid4(),
            "event_time": initial_time,
            "ingested_at": initial_time,
            "user_id": user_id,
            "content_id": "content_late_integration",
            "event_type": "play",
            "play_seconds": 30,
            "source": "web",
            "schema_version": "v1",
        }
    )

    progress_event = InteractionEventV1.model_validate(
        {
            **initial_event.model_dump(),
            "event_id": uuid4(),
            "event_time": progress_time,
            "ingested_at": progress_time,
            "user_id": f"integration_late_progress_{namespace.hex}",
            "event_type": "skip",
            "play_seconds": 0,
        }
    )

    # The progress event advances the watermark past the target window end.
    produce_events([initial_event, progress_event])

    initial_features = wait_for_features(
        observation_time=observation_time,
        expected={user_id: 1},
    )
    initial_feature = initial_features[user_id]

    assert initial_feature["feature_schema_version"] == "v1"
    assert initial_feature["feature_name"] == "user_play_count_1h"
    assert initial_feature["play_count"] == 1
    assert datetime.fromisoformat(
        initial_feature["window_start_exclusive"]
    ) == observation_time - timedelta(hours=1)

    # Submit only after the initial window output has been observed.
    late_event = InteractionEventV1.model_validate(
        {
            **initial_event.model_dump(),
            "event_id": uuid4(),
            "event_time": late_time,
            "ingested_at": progress_time + timedelta(seconds=1),
            "play_seconds": 45,
        }
    )

    expected_count = count_user_plays_1h(
        [initial_event, late_event],
        user_id=user_id,
        observation_time=observation_time,
    )
    assert expected_count == 2

    produce_events([late_event])

    corrected_features = wait_for_features(
        observation_time=observation_time,
        expected={user_id: expected_count},
    )
    corrected_feature = corrected_features[user_id]

    assert corrected_feature["feature_schema_version"] == "v1"
    assert corrected_feature["feature_name"] == "user_play_count_1h"
    assert corrected_feature["user_id"] == initial_feature["user_id"]
    assert corrected_feature["observation_time"] == initial_feature["observation_time"]
    assert corrected_feature["window_start_exclusive"] == initial_feature["window_start_exclusive"]
    assert corrected_feature["play_count"] == 2

    # Repeat the exact late event, then send a distinct play as a positive probe.
    next_event = InteractionEventV1.model_validate(
        {
            **initial_event.model_dump(),
            "event_id": uuid4(),
            "event_time": observation_time - timedelta(seconds=15),
            "ingested_at": progress_time + timedelta(seconds=2),
            "play_seconds": 60,
        }
    )

    assert next_event.event_id not in {
        initial_event.event_id,
        late_event.event_id,
    }

    # The reference is computed from the three distinct logical events.
    expected_after_duplicate = count_user_plays_1h(
        [initial_event, late_event, next_event],
        user_id=user_id,
        observation_time=observation_time,
    )
    assert expected_after_duplicate == 3

    produce_events([late_event, next_event])

    final_features = wait_for_features(
        observation_time=observation_time,
        expected={user_id: expected_after_duplicate},
    )
    final_feature = final_features[user_id]

    assert final_feature["feature_schema_version"] == "v1"
    assert final_feature["feature_name"] == "user_play_count_1h"
    assert final_feature["user_id"] == initial_feature["user_id"]
    assert final_feature["observation_time"] == initial_feature["observation_time"]
    assert final_feature["window_start_exclusive"] == initial_feature["window_start_exclusive"]
    assert final_feature["play_count"] == 3

    assert_no_excess_play_count(
        user_id=user_id,
        observation_time=observation_time,
        expected_count=expected_after_duplicate,
    )


@pytest.mark.integration
def test_event_too_late_for_all_windows_is_routed() -> None:
    """Route a valid event after every assigned window has expired."""
    late_topic = "interaction-events.user-play-count-1h.late.v1"
    namespace = uuid4()
    probe_user = f"integration_too_late_probe_{namespace.hex}"

    observation_time = datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(minutes=15)
    probe_time = observation_time - timedelta(minutes=1)
    progress_time = observation_time + timedelta(minutes=11)

    probe_event = InteractionEventV1.model_validate(
        {
            "event_id": uuid4(),
            "event_time": probe_time,
            "ingested_at": probe_time,
            "user_id": probe_user,
            "content_id": "content_too_late_integration",
            "event_type": "play",
            "play_seconds": 30,
            "source": "web",
            "schema_version": "v1",
        }
    )

    progress_event = InteractionEventV1.model_validate(
        {
            **probe_event.model_dump(),
            "event_id": uuid4(),
            "event_time": progress_time,
            "ingested_at": progress_time,
            "user_id": f"integration_too_late_progress_{namespace.hex}",
            "event_type": "skip",
            "play_seconds": 0,
        }
    )

    # Observe an event-time window output before sending the old event.
    produce_events([probe_event, progress_event])

    probe_features = wait_for_features(
        observation_time=observation_time,
        expected={probe_user: 1},
    )
    assert probe_features[probe_user]["play_count"] == 1

    # All assigned one-hour windows and their 30-minute retention period
    # have expired by the watermark already demonstrated above.
    old_event_time = observation_time - timedelta(hours=2)
    old_event = InteractionEventV1.model_validate(
        {
            **probe_event.model_dump(),
            "event_id": uuid4(),
            "event_time": old_event_time,
            "ingested_at": progress_time + timedelta(seconds=1),
            "user_id": f"integration_too_late_{namespace.hex}",
        }
    )

    produce_events([old_event])

    deadline = time.monotonic() + 60
    matched_record = None

    while time.monotonic() < deadline:
        output = run_kafka(
            "kafka-console-consumer.sh",
            "--bootstrap-server",
            BOOTSTRAP_SERVERS,
            "--topic",
            late_topic,
            "--from-beginning",
            "--timeout-ms",
            "3000",
        )

        for line in output.splitlines():
            if not line.strip():
                continue

            record = json.loads(line)
            raw_payload = record.get("raw_payload")
            if not isinstance(raw_payload, str):
                continue

            original_payload = json.loads(raw_payload)
            if original_payload.get("event_id") == str(old_event.event_id):
                matched_record = record
                break

        if matched_record is not None:
            break

        time.sleep(0.5)

    assert matched_record is not None, (
        f"No late-event record found in {late_topic!r} for event_id={old_event.event_id}"
    )
    assert matched_record["error_schema_version"] == "v1"
    assert matched_record["reason_code"] == "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS"
    assert matched_record["feature_name"] == "user_play_count_1h"
    assert InteractionEventV1.model_validate_json(matched_record["raw_payload"]) == old_event
