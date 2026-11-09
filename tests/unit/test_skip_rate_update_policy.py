"""Validate skip-rate payloads and same-window count ordering."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from featureforge_stream.contracts.user_skip_rate import (
    SkipRateUpdateConflictError,
    UserSkipRateFeatureV1,
    decide_skip_rate_update,
)

OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)


def make_feature(plays=1, skips=1, *, minutes=0, **overrides):
    observation = OBSERVATION + timedelta(minutes=minutes)
    denominator = plays + skips
    payload = {
        "user_id": "user_000001",
        "play_count": plays,
        "skip_count": skips,
        "skip_rate": skips / denominator if denominator else 0.0,
        "observation_time": observation,
        "window_start_exclusive": observation - timedelta(hours=24),
    }
    payload.update(overrides)
    return UserSkipRateFeatureV1.model_validate(payload)


@pytest.mark.parametrize(
    ("plays", "skips"),
    [(0, 0), (1, 0), (0, 1), (2, 1), (1, 2)],
)
def test_payload_json_round_trip(plays, skips):
    feature = make_feature(plays, skips)
    assert UserSkipRateFeatureV1.model_validate_json(feature.model_dump_json()) == feature


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("play_count", -1),
        ("play_count", True),
        ("play_count", 1.0),
        ("play_count", "1"),
        ("skip_count", -1),
        ("skip_count", False),
        ("skip_count", 1.0),
        ("skip_count", "1"),
        ("skip_rate", float("nan")),
        ("skip_rate", float("inf")),
        ("skip_rate", -0.1),
        ("skip_rate", 1.1),
        ("skip_rate", "0.5"),
        ("skip_rate", True),
        ("skip_rate", 0.9),
        ("user_id", ""),
        ("user_id", "user 1"),
        ("user_id", "user_1\n"),
        ("user_id", 123),
        ("observation_time", OBSERVATION.replace(tzinfo=None)),
        ("window_start_exclusive", OBSERVATION - timedelta(hours=1)),
        ("unexpected_field", "value"),
        ("feature_name", "user_play_count_1h"),
        ("feature_schema_version", "v2"),
    ],
)
def test_rejects_invalid_payload(field, value):
    with pytest.raises(ValidationError):
        make_feature(**{field: value})


def test_zero_denominator_requires_zero_rate():
    with pytest.raises(ValidationError, match="skip_rate"):
        make_feature(0, 0, skip_rate=0.5)


def test_equivalent_timestamp_offsets_normalize_to_utc():
    offset = timezone(timedelta(hours=2))
    feature = make_feature(
        observation_time=OBSERVATION.astimezone(offset),
        window_start_exclusive=(OBSERVATION - timedelta(hours=24)).astimezone(offset),
    )
    assert feature == make_feature()
    assert feature.observation_time.tzinfo == UTC


def test_first_write():
    assert decide_skip_rate_update(None, make_feature()) == "written"


@pytest.mark.parametrize(
    ("plays", "skips", "decision"),
    [
        (1, 1, "unchanged"),
        (2, 1, "written"),
        (1, 2, "written"),
        (2, 2, "written"),
        (0, 1, "stale"),
        (1, 0, "stale"),
        (0, 0, "stale"),
    ],
)
def test_same_window_count_ordering(plays, skips, decision):
    assert decide_skip_rate_update(make_feature(), make_feature(plays, skips)) == decision


@pytest.mark.parametrize(("plays", "skips"), [(2, 0), (0, 2)])
def test_incomparable_counts_raise_conflict(plays, skips):
    with pytest.raises(SkipRateUpdateConflictError, match="Incomparable"):
        decide_skip_rate_update(make_feature(), make_feature(plays, skips))


def test_newer_zero_window_replaces_older_nonzero_window():
    assert decide_skip_rate_update(make_feature(5, 5), make_feature(0, 0, minutes=1)) == ("written")


def test_older_window_is_stale_even_with_larger_counts():
    assert decide_skip_rate_update(make_feature(), make_feature(99, 99, minutes=-1)) == ("stale")


def test_different_users_cannot_be_compared():
    with pytest.raises(ValueError, match="different user"):
        decide_skip_rate_update(make_feature(), make_feature(user_id="other_user"))


def test_late_corrections_and_replay_follow_count_vectors():
    initial = make_feature(1, 1)
    more_plays = make_feature(2, 1)
    more_skips = make_feature(2, 2)

    assert more_plays.skip_rate < initial.skip_rate
    assert more_skips.skip_rate > more_plays.skip_rate
    assert decide_skip_rate_update(initial, more_plays) == "written"
    assert decide_skip_rate_update(more_plays, more_skips) == "written"
    assert decide_skip_rate_update(more_skips, initial) == "stale"
    assert decide_skip_rate_update(more_skips, more_skips) == "unchanged"
