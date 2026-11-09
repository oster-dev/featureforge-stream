"""Contract tests for the user_skip_rate_24h reference."""

from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.reference.user_play_count import EventIdentityConflictError
from featureforge_stream.reference.user_skip_rate import calculate_user_skip_rate_24h

OBSERVATION_TIME = datetime(2026, 1, 2, 12, tzinfo=UTC)


def make_event(
    event_id: int = 1,
    event_type: str = "skip",
    **changes: object,
) -> InteractionEventV1:
    payload = {
        "event_id": UUID(int=event_id),
        "event_time": OBSERVATION_TIME,
        "ingested_at": OBSERVATION_TIME,
        "user_id": "user_000001",
        "content_id": "content_000001",
        "event_type": event_type,
        "play_seconds": 0,
        "source": "web",
        "schema_version": "v1",
    }
    payload.update(changes)
    return InteractionEventV1.model_validate(payload)


def rate(events, **overrides):
    arguments = {
        "user_id": "user_000001",
        "observation_time": OBSERVATION_TIME,
    }
    arguments.update(overrides)
    return calculate_user_skip_rate_24h(events, **arguments)


@pytest.mark.parametrize(
    ("types", "expected"),
    [
        ([], 0.0),
        (["play"], 0.0),
        (["skip"], 1.0),
        (["play", "skip"], 0.5),
        (["play", "play", "skip"], 1 / 3),
        (["skip", "skip", "play"], 2 / 3),
        (["impression", "complete"], 0.0),
        (["play", "skip", "impression", "complete"], 0.5),
    ],
)
def test_hand_calculated_rates(types: list[str], expected: float) -> None:
    events = [
        make_event(event_id=index, event_type=kind) for index, kind in enumerate(types, start=1)
    ]
    actual = rate(events)
    assert actual == pytest.approx(expected)
    assert isinstance(actual, float)
    assert 0.0 <= actual <= 1.0


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (timedelta(hours=-24, microseconds=-1), 0.0),
        (timedelta(hours=-24), 0.0),
        (timedelta(hours=-24, microseconds=1), 1.0),
        (timedelta(0), 1.0),
        (timedelta(microseconds=1), 0.0),
    ],
)
def test_window_boundaries(offset: timedelta, expected: float) -> None:
    timestamp = OBSERVATION_TIME + offset
    event = make_event(event_time=timestamp, ingested_at=timestamp)
    assert rate([event]) == expected


def test_other_users_do_not_contribute() -> None:
    events = [
        make_event(1, "play"),
        make_event(2, "skip", user_id="other_user"),
    ]
    assert rate(events) == 0.0


def test_duplicate_delivery_does_not_change_ratio() -> None:
    skip = make_event(1, "skip")
    repeated = make_event(
        1,
        "skip",
        ingested_at=OBSERVATION_TIME + timedelta(minutes=5),
    )
    assert rate([skip, repeated, skip, make_event(2, "play")]) == 0.5


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("content_id", "content_000002"),
        ("event_type", "play"),
        ("play_seconds", 10),
        ("source", "tv"),
        ("event_time", OBSERVATION_TIME - timedelta(seconds=1)),
        ("user_id", "other_user"),
    ],
)
def test_conflicting_payload_is_rejected(field: str, value: object) -> None:
    with pytest.raises(EventIdentityConflictError, match="event_id="):
        rate([make_event(), make_event(**{field: value})])


@pytest.mark.parametrize("reverse", [False, True])
def test_conflict_outside_query_still_fails(reverse: bool) -> None:
    timestamp = OBSERVATION_TIME - timedelta(days=2)
    events = [
        make_event(
            event_time=timestamp,
            user_id="other_user",
            event_type="impression",
        ),
        make_event(
            event_time=timestamp,
            user_id="other_user",
            event_type="complete",
        ),
    ]
    if reverse:
        events.reverse()
    with pytest.raises(EventIdentityConflictError, match="event_id="):
        rate(events)


def test_generator_order_independence_and_no_mutation() -> None:
    events = [make_event(1, "skip"), make_event(2, "play"), make_event(3, "play")]
    before = [event.model_dump() for event in events]
    assert rate(events) == pytest.approx(1 / 3)
    assert rate(event for event in reversed(events)) == pytest.approx(1 / 3)
    assert [event.model_dump() for event in events] == before


def test_equivalent_observation_offset() -> None:
    local_time = OBSERVATION_TIME.astimezone(timezone(timedelta(hours=2)))
    assert rate([make_event()], observation_time=local_time) == 1.0


def test_receipt_after_observation_does_not_exclude_event() -> None:
    event = make_event(ingested_at=OBSERVATION_TIME + timedelta(hours=2))
    assert rate([event]) == 1.0


def test_expiry_recomputes_numerator_and_denominator() -> None:
    events = [
        make_event(1, "skip", event_time=OBSERVATION_TIME - timedelta(hours=23)),
        make_event(2, "play"),
    ]
    assert rate(events) == 0.5
    assert (
        rate(
            events,
            observation_time=OBSERVATION_TIME + timedelta(hours=1),
        )
        == 0.0
    )


@pytest.mark.parametrize("user_id", ["", " ", "user 1", "user_1\n"])
def test_rejects_invalid_user_id(user_id: str) -> None:
    with pytest.raises(ValueError, match="user_id"):
        rate([], user_id=user_id)


@pytest.mark.parametrize("user_id", [None, 123])
def test_rejects_non_string_user_id(user_id: object) -> None:
    with pytest.raises(TypeError, match="user_id"):
        rate([], user_id=user_id)


def test_rejects_naive_observation_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        rate([], observation_time=datetime(2026, 1, 2, 12))


def test_rejects_non_datetime_observation() -> None:
    with pytest.raises(TypeError, match="observation_time"):
        rate([], observation_time="2026-01-02T12:00:00Z")


def test_rejects_invalid_item_even_after_valid_events() -> None:
    with pytest.raises(TypeError, match="InteractionEventV1"):
        rate([make_event(), {}])
