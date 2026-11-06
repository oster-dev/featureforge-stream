"""Valid events and rejected payloads are routed separately."""

import json
from pathlib import Path

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.contracts.routing import route_event_json

FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "user_play_count_1h_v1.jsonl"


@pytest.fixture
def valid_json() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8").splitlines()[0]


def test_valid_event_has_only_a_valid_result(valid_json: str) -> None:
    valid, rejected = route_event_json(valid_json)

    assert valid is not None
    assert rejected is None
    assert InteractionEventV1.model_validate_json(valid) == (
        InteractionEventV1.model_validate_json(valid_json)
    )


@pytest.mark.parametrize(
    ("payload", "reason_code"),
    [
        ('{"broken":', "INVALID_JSON"),
        ('{"event_type":"unsupported"}', "EVENT_CONTRACT_VIOLATION"),
    ],
)
def test_invalid_input_preserves_payload_and_reason(payload: str, reason_code: str) -> None:
    valid, rejected = route_event_json(payload)

    assert valid is None
    assert rejected is not None

    error = json.loads(rejected)

    assert error["error_schema_version"] == "v1"
    assert error["reason_code"] == reason_code
    assert error["raw_payload"] == payload
    assert error["errors"]
    assert all("input" not in item for item in error["errors"])


def test_invalid_record_does_not_block_the_next_valid_record(valid_json: str) -> None:
    results = [route_event_json(payload) for payload in [valid_json, '{"broken":', valid_json]]

    assert [valid is not None for valid, _ in results] == [True, False, True]
    assert [rejected is not None for _, rejected in results] == [False, True, False]


def test_utc_normalization_is_preserved(valid_json: str) -> None:
    payload = json.loads(valid_json)
    payload["event_time"] = "2026-01-01T13:00:00+02:00"
    payload["ingested_at"] = "2026-01-01T13:01:00+02:00"

    valid, rejected = route_event_json(json.dumps(payload))

    assert rejected is None
    assert valid is not None
    assert json.loads(valid)["event_time"] == "2026-01-01T11:00:00Z"
