"""Reference feature semantics and golden-fixture checks."""

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.reference.user_play_count import (
    EventIdentityConflictError,
    count_user_plays_1h,
)

OBSERVATION_TIME = datetime(2026, 1, 1, 12, tzinfo=UTC)
FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "user_play_count_1h_v1.jsonl"


def make_event(
    *,
    event_id: int = 1,
    event_time: datetime = OBSERVATION_TIME,
    user_id: str = "user_000001",
    event_type: str = "play",
    ingested_at: datetime | None = None,
) -> InteractionEventV1:
    return InteractionEventV1.model_validate(
        {
            "event_id": UUID(int=event_id),
            "event_time": event_time,
            "ingested_at": event_time if ingested_at is None else ingested_at,
            "user_id": user_id,
            "content_id": "content_000001",
            "event_type": event_type,
            "play_seconds": 0,
            "source": "web",
            "schema_version": "v1",
        }
    )


@pytest.fixture
def golden_events() -> list[InteractionEventV1]:
    return [
        InteractionEventV1.model_validate_json(line)
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@pytest.mark.parametrize(
    ("user_id", "expected"),
    [
        ("user_000001", 3),
        ("user_000002", 1),
        ("user_unknown", 0),
    ],
)
def test_golden_counts(
    golden_events: list[InteractionEventV1], user_id: str, expected: int
) -> None:
    assert (
        count_user_plays_1h(
            golden_events,
            user_id=user_id,
            observation_time=OBSERVATION_TIME,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (timedelta(hours=-1, microseconds=-1), 0),
        (timedelta(hours=-1), 0),
        (timedelta(hours=-1, microseconds=1), 1),
        (timedelta(0), 1),
        (timedelta(microseconds=1), 0),
    ],
)
def test_window_boundaries(offset: timedelta, expected: int) -> None:
    event = make_event(event_time=OBSERVATION_TIME + offset)

    assert (
        count_user_plays_1h(
            [event],
            user_id="user_000001",
            observation_time=OBSERVATION_TIME,
        )
        == expected
    )


@pytest.mark.parametrize("event_type", ["impression", "skip", "complete"])
def test_excludes_non_play_events(event_type: str) -> None:
    assert (
        count_user_plays_1h(
            [make_event(event_type=event_type)],
            user_id="user_000001",
            observation_time=OBSERVATION_TIME,
        )
        == 0
    )


def test_empty_input_returns_zero() -> None:
    assert (
        count_user_plays_1h(
            [],
            user_id="user_000001",
            observation_time=OBSERVATION_TIME,
        )
        == 0
    )


def test_repeated_delivery_counts_once() -> None:
    first = make_event()
    repeated = make_event(ingested_at=OBSERVATION_TIME + timedelta(minutes=5))

    assert (
        count_user_plays_1h(
            [first, repeated, first],
            user_id="user_000001",
            observation_time=OBSERVATION_TIME,
        )
        == 1
    )


@pytest.mark.parametrize("reverse", [False, True])
def test_conflict_outside_query_still_fails(reverse: bool) -> None:
    events = [
        make_event(
            event_time=OBSERVATION_TIME - timedelta(hours=2),
            user_id="other_user",
        ),
        make_event(
            event_time=OBSERVATION_TIME - timedelta(hours=2),
            user_id="conflicting_user",
        ),
    ]
    if reverse:
        events.reverse()

    with pytest.raises(EventIdentityConflictError, match="event_id="):
        count_user_plays_1h(
            events,
            user_id="user_000001",
            observation_time=OBSERVATION_TIME,
        )


def test_order_independence_and_no_mutation(
    golden_events: list[InteractionEventV1],
) -> None:
    before = [event.model_dump() for event in golden_events]

    assert (
        count_user_plays_1h(
            reversed(golden_events),
            user_id="user_000001",
            observation_time=OBSERVATION_TIME,
        )
        == 3
    )

    assert [event.model_dump() for event in golden_events] == before


def test_accepts_equivalent_observation_offset(
    golden_events: list[InteractionEventV1],
) -> None:
    local_time = OBSERVATION_TIME.astimezone(timezone(timedelta(hours=2)))

    assert (
        count_user_plays_1h(
            golden_events,
            user_id="user_000001",
            observation_time=local_time,
        )
        == 3
    )


def test_reconstructs_event_time_even_if_received_after_observation() -> None:
    event = make_event(
        event_time=OBSERVATION_TIME - timedelta(minutes=5),
        ingested_at=OBSERVATION_TIME + timedelta(minutes=20),
    )

    assert (
        count_user_plays_1h(
            [event],
            user_id="user_000001",
            observation_time=OBSERVATION_TIME,
        )
        == 1
    )


def test_rejects_naive_observation_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        count_user_plays_1h(
            [],
            user_id="user_000001",
            observation_time=datetime(2026, 1, 1, 12),
        )


@pytest.mark.parametrize("user_id", ["", " ", "user 1", "user_1\n"])
def test_rejects_invalid_user_id(user_id: str) -> None:
    with pytest.raises(ValueError, match="user_id"):
        count_user_plays_1h(
            [],
            user_id=user_id,
            observation_time=OBSERVATION_TIME,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("content_id", "content_000002"),
        ("event_type", "skip"),
        ("play_seconds", 10),
        ("source", "tv"),
        ("event_time", OBSERVATION_TIME - timedelta(seconds=1)),
    ],
)
def test_rejects_conflicting_logical_payload(field: str, value: object) -> None:
    original = make_event()
    changed_payload = original.model_dump()
    changed_payload[field] = value
    conflicting = InteractionEventV1.model_validate(changed_payload)

    with pytest.raises(EventIdentityConflictError, match="event_id="):
        count_user_plays_1h(
            [original, conflicting],
            user_id="user_000001",
            observation_time=OBSERVATION_TIME,
        )


def test_play_expires_when_observation_time_advances() -> None:
    event = make_event(event_time=OBSERVATION_TIME)

    before_expiry = count_user_plays_1h(
        [event],
        user_id="user_000001",
        observation_time=OBSERVATION_TIME + timedelta(minutes=59),
    )
    at_expiry = count_user_plays_1h(
        [event],
        user_id="user_000001",
        observation_time=OBSERVATION_TIME + timedelta(hours=1),
    )

    assert before_expiry == 1
    assert at_expiry == 0
