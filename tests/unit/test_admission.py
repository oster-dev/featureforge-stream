"""Arrival-order admission semantics."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.reference.admission import (
    Admission,
    TimePolicy,
    admit_in_arrival_order,
    classify_event_time,
)
from featureforge_stream.reference.user_play_count import count_user_plays_1h

BASE = datetime(2026, 1, 1, 12, tzinfo=UTC)
POLICY = TimePolicy(
    watermark_delay=timedelta(minutes=10),
    allowed_lateness=timedelta(minutes=30),
)


def make_event(event_id: int, event_time: datetime) -> InteractionEventV1:
    return InteractionEventV1.model_validate(
        {
            "event_id": UUID(int=event_id),
            "event_time": event_time,
            "ingested_at": event_time,
            "user_id": "user_000001",
            "content_id": "content_000001",
            "event_type": "play",
            "play_seconds": 0,
            "source": "web",
            "schema_version": "v1",
        }
    )


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (timedelta(minutes=5), Admission.ON_TIME),
        (timedelta(0), Admission.ON_TIME),
        (timedelta(minutes=-10), Admission.ON_TIME),
        (timedelta(minutes=-10, microseconds=-1), Admission.ALLOWED_LATE),
        (timedelta(minutes=-40), Admission.ALLOWED_LATE),
        (timedelta(minutes=-40, microseconds=-1), Admission.TOO_LATE),
    ],
)
def test_classification_boundaries(offset: timedelta, expected: Admission) -> None:
    assert classify_event_time(BASE + offset, BASE, POLICY) is expected


def test_first_event_is_on_time() -> None:
    assert classify_event_time(BASE, None, POLICY) is Admission.ON_TIME


def test_zero_lateness_policy_has_no_allowed_late_band() -> None:
    policy = TimePolicy(watermark_delay=timedelta(0), allowed_lateness=timedelta(0))

    assert classify_event_time(BASE - timedelta(microseconds=1), BASE, policy) is Admission.TOO_LATE


def test_decisions_preserve_arrival_order_and_boundary() -> None:
    events = [make_event(1, BASE), make_event(2, BASE - timedelta(minutes=15))]

    decisions = admit_in_arrival_order(events, policy=POLICY)

    assert [decision.event for decision in decisions] == events
    assert decisions[0].boundary is None
    assert decisions[1].boundary == BASE - timedelta(minutes=10)
    assert decisions[1].admission is Admission.ALLOWED_LATE


def test_arrival_order_changes_the_outcome() -> None:
    newer = make_event(1, BASE)
    older = make_event(2, BASE - timedelta(minutes=50))

    newer_first = admit_in_arrival_order([newer, older], policy=POLICY)
    older_first = admit_in_arrival_order([older, newer], policy=POLICY)

    assert newer_first[1].admission is Admission.TOO_LATE
    assert [d.admission for d in older_first] == [Admission.ON_TIME, Admission.ON_TIME]


def test_too_late_event_does_not_advance_the_maximum() -> None:
    events = [
        make_event(1, BASE),
        make_event(2, BASE - timedelta(minutes=50)),
        make_event(3, BASE - timedelta(minutes=15)),
    ]

    decisions = admit_in_arrival_order(events, policy=POLICY)

    assert [d.admission for d in decisions] == [
        Admission.ON_TIME,
        Admission.TOO_LATE,
        Admission.ALLOWED_LATE,
    ]


def test_empty_input_returns_no_decisions() -> None:
    assert admit_in_arrival_order([], policy=POLICY) == ()


def test_rejects_non_event_items() -> None:
    with pytest.raises(TypeError, match="InteractionEventV1"):
        admit_in_arrival_order([object()], policy=POLICY)  # type: ignore[list-item]


@pytest.mark.parametrize(
    ("delay", "lateness"),
    [(timedelta(seconds=-1), timedelta(0)), (timedelta(0), timedelta(seconds=-1))],
)
def test_rejects_negative_policy_values(delay: timedelta, lateness: timedelta) -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        TimePolicy(watermark_delay=delay, allowed_lateness=lateness)


def test_admitted_subset_changes_the_feature_value() -> None:
    events = [make_event(1, BASE), make_event(2, BASE - timedelta(minutes=55))]
    decisions = admit_in_arrival_order(events, policy=POLICY)
    admitted = [d.event for d in decisions if d.admission is not Admission.TOO_LATE]

    assert [d.admission for d in decisions] == [Admission.ON_TIME, Admission.TOO_LATE]
    assert count_user_plays_1h(events, user_id="user_000001", observation_time=BASE) == 2
    assert count_user_plays_1h(admitted, user_id="user_000001", observation_time=BASE) == 1
