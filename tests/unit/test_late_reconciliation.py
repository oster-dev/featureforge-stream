"""Verify strict late-output matching and preservation of the full comparison."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.online_store import UserPlayCountFeatureV1
from featureforge_stream.reference.reconciliation import reconcile_late_output

OBSERVATION = datetime(2026, 1, 1, 12, tzinfo=UTC)


def make_event(number, user_id):
    return InteractionEventV1(
        event_id=UUID(int=number),
        event_time=OBSERVATION,
        ingested_at=OBSERVATION + timedelta(hours=3),
        user_id=user_id,
        content_id="content_1",
        event_type="play",
        play_seconds=30,
        source="web",
        schema_version="v1",
    )


def make_windows(user_id):
    return [
        UserPlayCountFeatureV1(
            user_id=user_id,
            play_count=1,
            observation_time=OBSERVATION + timedelta(minutes=minute),
            window_start_exclusive=(OBSERVATION + timedelta(minutes=minute) - timedelta(hours=1)),
        )
        for minute in range(60)
    ]


def envelope(event):
    return {
        "error_schema_version": "v1",
        "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
        "feature_name": "user_play_count_1h",
        "raw_payload": event.model_dump_json(),
    }


def test_preserves_failure_and_explains_it_with_verified_exclusion():
    retained = make_event(1, "user_1")
    excluded = make_event(2, "user_2")

    report = reconcile_late_output(
        [retained, excluded],
        make_windows("user_1"),
        [envelope(excluded)],
    )

    assert report["full_input_comparison"]["passed"] is False
    assert report["full_input_comparison"]["missing_window_count"] == 60
    assert report["comparison_after_observed_exclusions"]["passed"] is True
    assert report["consistent_with_observed_exclusions"] is True
    assert report["observed_late_output"]["unique_excluded_event_ids"] == 1
    assert report["watermark_decisions_independently_verified"] is False


def test_no_exclusions_does_not_hide_missing_windows():
    retained = make_event(1, "user_1")
    other = make_event(2, "user_2")

    report = reconcile_late_output([retained, other], make_windows("user_1"), [])

    assert report["consistent_with_observed_exclusions"] is False
    assert report["comparison_after_observed_exclusions"]["missing_window_count"] == 60


def test_complete_input_without_exclusions_passes():
    event = make_event(1, "user_1")

    report = reconcile_late_output([event], make_windows("user_1"), [])

    assert report["full_input_comparison"]["passed"] is True
    assert report["consistent_with_observed_exclusions"] is True


@pytest.mark.parametrize(
    "field,value",
    [
        ("reason_code", "OTHER"),
        ("feature_name", "other_feature"),
        ("error_schema_version", "v2"),
    ],
)
def test_rejects_unsupported_late_envelope(field, value):
    event = make_event(1, "user_1")
    record = envelope(event)
    record[field] = value

    with pytest.raises(ValueError, match="Unsupported late-output"):
        reconcile_late_output([event], make_windows("user_1"), [record])


def test_rejects_unknown_late_event():
    source = make_event(1, "user_1")
    unknown = make_event(2, "user_2")

    with pytest.raises(ValueError, match="does not match a source delivery"):
        reconcile_late_output([source], make_windows("user_1"), [envelope(unknown)])


def test_rejects_same_id_with_changed_receipt_payload():
    source = make_event(1, "user_1")
    changed = InteractionEventV1.model_validate(
        {
            **source.model_dump(),
            "ingested_at": source.ingested_at + timedelta(seconds=1),
        }
    )

    with pytest.raises(ValueError, match="does not match a source delivery"):
        reconcile_late_output([source], make_windows("user_1"), [envelope(changed)])


def test_rejects_duplicate_late_event_id():
    retained = make_event(1, "user_1")
    excluded = make_event(2, "user_2")
    record = envelope(excluded)

    with pytest.raises(ValueError, match="Duplicate late-output"):
        reconcile_late_output([retained, excluded], make_windows("user_1"), [record, record])


def test_rejects_exclusion_of_entire_source():
    event = make_event(1, "user_1")

    with pytest.raises(ValueError, match="No source events remain"):
        reconcile_late_output([event], make_windows("user_1"), [envelope(event)])


def test_verified_exclusion_does_not_hide_wrong_retained_counts():
    retained = make_event(1, "user_1")
    excluded = make_event(2, "user_2")
    windows = make_windows("user_1")
    windows[0] = UserPlayCountFeatureV1.model_validate({**windows[0].model_dump(), "play_count": 2})

    report = reconcile_late_output([retained, excluded], windows, [envelope(excluded)])

    assert report["consistent_with_observed_exclusions"] is False
    assert report["comparison_after_observed_exclusions"]["mismatch_count"] == 1
