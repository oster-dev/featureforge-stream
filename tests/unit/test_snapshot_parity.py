"""Test source-to-snapshot comparison against the existing golden reference."""

from datetime import UTC, datetime, timedelta

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.online_store import UserPlayCountFeatureV1
from featureforge_stream.reference.snapshot_parity import compare_snapshot_with_events
from featureforge_stream.reference.user_play_count import (
    EventIdentityConflictError,
    count_user_plays_1h,
)

OBSERVATION = datetime(2026, 1, 1, 12, tzinfo=UTC)


@pytest.fixture
def events():
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "fixtures" / "user_play_count_1h_v1.jsonl"
    return [
        InteractionEventV1.model_validate_json(line)
        for line in path.read_text().splitlines()
        if line.strip()
    ]


def make_window(*, user_id="user_000001", count=3, observation=OBSERVATION):
    return UserPlayCountFeatureV1(
        user_id=user_id,
        play_count=count,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=1),
    )


def test_golden_values_and_duplicate_accounting(events):
    windows = [
        make_window(),
        make_window(user_id="user_000002", count=1),
        make_window(user_id="user_unknown", count=0),
    ]

    report = compare_snapshot_with_events(events, windows)

    assert report["passed"] is True
    assert report["source_deliveries"] == 8
    assert report["unique_source_events"] == 7
    assert report["duplicate_deliveries"] == 1
    assert report["snapshot_windows_checked"] == 3
    assert report["missing_window_coverage_checked"] is False
    assert report["flink_admission_simulated"] is False


@pytest.mark.parametrize("user_id", ["user_000001", "user_000002", "user_unknown"])
def test_indexed_counts_match_existing_reference_across_windows(events, user_id):
    observations = [OBSERVATION + timedelta(minutes=minute) for minute in range(-61, 62)]
    windows = [
        make_window(
            user_id=user_id,
            count=count_user_plays_1h(
                events,
                user_id=user_id,
                observation_time=observation,
            ),
            observation=observation,
        )
        for observation in observations
    ]

    report = compare_snapshot_with_events(events, windows)

    assert report["passed"] is True
    assert report["snapshot_windows_checked"] == len(observations)


@pytest.mark.parametrize("actual", [2, 4])
def test_mismatches_are_reported_not_filtered(events, actual):
    report = compare_snapshot_with_events(events, [make_window(count=actual)])

    assert report["passed"] is False
    assert report["mismatch_count"] == 1
    assert report["snapshot_count_below_reference"] == int(actual < 3)
    assert report["snapshot_count_above_reference"] == int(actual > 3)
    assert report["mismatch_examples"][0]["expected_source_count"] == 3
    assert report["mismatch_examples"][0]["actual_snapshot_count"] == actual


def test_source_order_and_repeated_delivery_do_not_change_counts(events):
    window = make_window()

    assert compare_snapshot_with_events(reversed(events), [window])["passed"]
    assert compare_snapshot_with_events(events + events, [window])["passed"]


def test_later_receipt_time_does_not_exclude_event(events):
    changed = [
        InteractionEventV1.model_validate(
            {
                **event.model_dump(),
                "ingested_at": OBSERVATION + timedelta(days=1),
            }
        )
        for event in events
    ]

    assert compare_snapshot_with_events(changed, [make_window()])["passed"]


def test_identity_conflict_is_rejected_even_outside_requested_user(events):
    conflict = InteractionEventV1.model_validate(
        {
            **events[0].model_dump(),
            "content_id": "conflicting_content",
        }
    )

    with pytest.raises(EventIdentityConflictError):
        compare_snapshot_with_events(
            [*events, conflict],
            [make_window(user_id="user_unknown", count=0)],
        )


def test_empty_source_is_rejected():
    with pytest.raises(ValueError, match="empty event source"):
        compare_snapshot_with_events([], [make_window()])


def test_empty_snapshot_is_rejected(events):
    with pytest.raises(ValueError, match="empty snapshot"):
        compare_snapshot_with_events(events, [])


def test_unvalidated_source_is_rejected():
    with pytest.raises(TypeError, match="InteractionEventV1"):
        compare_snapshot_with_events([{}], [make_window()])


def test_duplicate_snapshot_keys_are_rejected(events):
    with pytest.raises(ValueError, match="unique and sorted"):
        compare_snapshot_with_events(events, [make_window(), make_window()])


def test_unsorted_snapshot_is_rejected(events):
    with pytest.raises(ValueError, match="unique and sorted"):
        compare_snapshot_with_events(
            events,
            [
                make_window(observation=OBSERVATION + timedelta(minutes=1)),
                make_window(),
            ],
        )


def coverage_event(events, *, event_time, event_type="play"):
    return InteractionEventV1.model_validate(
        {
            **events[0].model_dump(),
            "event_time": event_time,
            "ingested_at": event_time,
            "event_type": event_type,
            "play_seconds": 0,
        }
    )


@pytest.mark.parametrize(
    "event_time,first_end",
    [
        (OBSERVATION, OBSERVATION),
        (
            OBSERVATION + timedelta(microseconds=1),
            OBSERVATION + timedelta(minutes=1),
        ),
        (
            OBSERVATION - timedelta(microseconds=1),
            OBSERVATION,
        ),
    ],
)
def test_complete_window_coverage_at_minute_boundaries(events, event_time, first_end):
    from featureforge_stream.reference.snapshot_parity import compare_snapshot_with_coverage

    event = coverage_event(events, event_time=event_time)
    windows = [
        make_window(
            count=1,
            observation=first_end + timedelta(minutes=minute),
        )
        for minute in range(60)
    ]

    report = compare_snapshot_with_coverage([event], windows)

    assert report["passed"] is True
    assert report["expected_snapshot_windows"] == 60
    assert report["missing_window_count"] == 0
    assert report["unexpected_window_count"] == 0
    assert report["missing_window_coverage_checked"] is True


def test_missing_window_fails_even_when_remaining_counts_match(events):
    from featureforge_stream.reference.snapshot_parity import compare_snapshot_with_coverage

    event = coverage_event(events, event_time=OBSERVATION)
    windows = [
        make_window(count=1, observation=OBSERVATION + timedelta(minutes=minute))
        for minute in range(1, 60)
    ]

    report = compare_snapshot_with_coverage([event], windows)

    assert report["mismatch_count"] == 0
    assert report["passed"] is False
    assert report["missing_window_count"] == 1
    assert report["unexpected_window_count"] == 0
    assert report["missing_window_examples"][0]["observation_time"] == OBSERVATION.isoformat()


def test_unexpected_zero_window_fails_even_when_counts_match(events):
    from featureforge_stream.reference.snapshot_parity import compare_snapshot_with_coverage

    event = coverage_event(events, event_time=OBSERVATION)
    windows = [
        make_window(count=1, observation=OBSERVATION + timedelta(minutes=minute))
        for minute in range(60)
    ]
    windows.append(make_window(count=0, observation=OBSERVATION + timedelta(minutes=60)))

    report = compare_snapshot_with_coverage([event], windows)

    assert report["mismatch_count"] == 0
    assert report["passed"] is False
    assert report["missing_window_count"] == 0
    assert report["unexpected_window_count"] == 1


def test_non_play_event_creates_zero_count_windows(events):
    from featureforge_stream.reference.snapshot_parity import compare_snapshot_with_coverage

    event = coverage_event(events, event_time=OBSERVATION, event_type="impression")
    windows = [
        make_window(count=0, observation=OBSERVATION + timedelta(minutes=minute))
        for minute in range(60)
    ]

    report = compare_snapshot_with_coverage([event], windows)

    assert report["passed"] is True
    assert report["expected_snapshot_windows"] == 60


def test_duplicate_deliveries_do_not_expand_expected_windows(events):
    from featureforge_stream.reference.snapshot_parity import compare_snapshot_with_coverage

    event = coverage_event(events, event_time=OBSERVATION)
    windows = [
        make_window(count=1, observation=OBSERVATION + timedelta(minutes=minute))
        for minute in range(60)
    ]

    report = compare_snapshot_with_coverage([event, event], windows)

    assert report["passed"] is True
    assert report["duplicate_deliveries"] == 1
    assert report["expected_snapshot_windows"] == 60
