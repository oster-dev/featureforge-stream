"""Executable examples of the interaction-events.v1 contract."""

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from featureforge_stream.contracts.events import InteractionEventV1


@pytest.fixture
def valid_payload() -> dict[str, object]:
    return {
        "event_id": "00000000-0000-4000-8000-000000000001",
        "event_time": "2026-01-01T10:00:00Z",
        "ingested_at": "2026-01-01T10:01:00Z",
        "user_id": "user_000001",
        "content_id": "content_000001",
        "event_type": "play",
        "play_seconds": 0,
        "source": "web",
        "schema_version": "v1",
    }


def test_accepts_valid_event(valid_payload: dict[str, object]) -> None:
    event = InteractionEventV1.model_validate(valid_payload)

    assert event.event_id == UUID(str(valid_payload["event_id"]))
    assert event.event_time == datetime(2026, 1, 1, 10, tzinfo=UTC)
    assert event.ingested_at - event.event_time == timedelta(minutes=1)
    assert event.user_id == "user_000001"


def test_accepts_json_and_round_trips(valid_payload: dict[str, object]) -> None:
    event = InteractionEventV1.model_validate_json(json.dumps(valid_payload))

    restored = InteractionEventV1.model_validate_json(event.model_dump_json())

    assert restored == event


@pytest.mark.parametrize("event_type", ["impression", "play", "skip", "complete"])
def test_accepts_supported_event_types(valid_payload: dict[str, object], event_type: str) -> None:
    valid_payload["event_type"] = event_type

    assert InteractionEventV1.model_validate(valid_payload).event_type == event_type


@pytest.mark.parametrize("source", ["web", "mobile", "tv"])
def test_accepts_supported_sources(valid_payload: dict[str, object], source: str) -> None:
    valid_payload["source"] = source

    assert InteractionEventV1.model_validate(valid_payload).source == source


def test_normalizes_offsets_to_utc(valid_payload: dict[str, object]) -> None:
    valid_payload["event_time"] = "2026-01-01T12:00:00+02:00"
    valid_payload["ingested_at"] = "2026-01-01T12:01:00+02:00"

    event = InteractionEventV1.model_validate(valid_payload)

    assert event.event_time == datetime(2026, 1, 1, 10, tzinfo=UTC)
    assert event.event_time.utcoffset() == timedelta(0)
    assert event.ingested_at.utcoffset() == timedelta(0)


def test_accepts_equal_event_and_ingestion_times(valid_payload: dict[str, object]) -> None:
    valid_payload["ingested_at"] = valid_payload["event_time"]

    event = InteractionEventV1.model_validate(valid_payload)

    assert event.ingested_at == event.event_time


@pytest.mark.parametrize("field", ["event_time", "ingested_at"])
@pytest.mark.parametrize(
    "value",
    [
        "2026-01-01T10:00:00",
        "2026-01-01",
        "not-a-timestamp",
        1767261600,
        True,
        datetime(2026, 1, 1, 10),
    ],
)
def test_rejects_invalid_timestamps(
    valid_payload: dict[str, object], field: str, value: object
) -> None:
    valid_payload[field] = value

    with pytest.raises(ValidationError):
        InteractionEventV1.model_validate(valid_payload)


@pytest.mark.parametrize("field", ["user_id", "content_id"])
@pytest.mark.parametrize("value", ["", " ", " user_1", "user 1", "user_1\n", 123])
def test_rejects_invalid_entity_ids(
    valid_payload: dict[str, object], field: str, value: object
) -> None:
    valid_payload[field] = value

    with pytest.raises(ValidationError):
        InteractionEventV1.model_validate(valid_payload)


@pytest.mark.parametrize("value", [-1, 1.5, 1.0, "10", True, None])
def test_rejects_invalid_play_seconds(valid_payload: dict[str, object], value: object) -> None:
    valid_payload["play_seconds"] = value

    with pytest.raises(ValidationError):
        InteractionEventV1.model_validate(valid_payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "not-a-uuid"),
        ("event_id", 123),
        ("event_type", "watch"),
        ("source", "console"),
        ("schema_version", "v2"),
    ],
)
def test_rejects_unsupported_values(
    valid_payload: dict[str, object], field: str, value: object
) -> None:
    valid_payload[field] = value

    with pytest.raises(ValidationError):
        InteractionEventV1.model_validate(valid_payload)


def test_rejects_ingestion_before_event(valid_payload: dict[str, object]) -> None:
    valid_payload["ingested_at"] = "2026-01-01T09:59:59Z"

    with pytest.raises(ValidationError, match="ingested_at must be on or after"):
        InteractionEventV1.model_validate(valid_payload)


def test_rejects_impression_with_play_seconds(valid_payload: dict[str, object]) -> None:
    valid_payload.update(event_type="impression", play_seconds=1)

    with pytest.raises(ValidationError, match="impression events must"):
        InteractionEventV1.model_validate(valid_payload)


@pytest.mark.parametrize(
    "field",
    [
        "event_id",
        "event_time",
        "ingested_at",
        "user_id",
        "content_id",
        "event_type",
        "play_seconds",
        "source",
        "schema_version",
    ],
)
def test_requires_every_contract_field(valid_payload: dict[str, object], field: str) -> None:
    del valid_payload[field]

    with pytest.raises(ValidationError) as exc_info:
        InteractionEventV1.model_validate(valid_payload)

    assert any(
        error["loc"] == (field,) and error["type"] == "missing" for error in exc_info.value.errors()
    )


def test_rejects_unknown_fields(valid_payload: dict[str, object]) -> None:
    valid_payload["unexpected_field"] = "unexpected"

    with pytest.raises(ValidationError) as exc_info:
        InteractionEventV1.model_validate(valid_payload)

    assert any(error["type"] == "extra_forbidden" for error in exc_info.value.errors())


def test_prevents_normal_field_assignment(valid_payload: dict[str, object]) -> None:
    event = InteractionEventV1.model_validate(valid_payload)

    with pytest.raises(ValidationError, match="frozen"):
        event.play_seconds = 100
