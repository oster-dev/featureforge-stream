"""Compare snapshot windows with independent event-time reconstruction.

This comparison does not simulate Flink watermark emission, window cleanup,
or processing-time deduplication TTL. Mismatches are reported, not filtered.
"""

from bisect import bisect_right
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from uuid import UUID

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.online_store import UserPlayCountFeatureV1
from featureforge_stream.reference.user_play_count import EventIdentityConflictError
from featureforge_stream.snapshot import consolidate_windows


def compare_snapshot_with_events(
    events: Iterable[InteractionEventV1],
    windows: Iterable[UserPlayCountFeatureV1],
) -> dict:
    """Compare all supplied windows with unique source plays in (start, end].

    Validate event identity across the entire supplied source before filtering.
    Receipt timestamps do not affect event-time reconstruction.
    """
    unique: dict[UUID, InteractionEventV1] = {}
    deliveries = 0

    for event in events:
        if not isinstance(event, InteractionEventV1):
            raise TypeError("events must contain InteractionEventV1 instances")

        deliveries += 1
        previous = unique.get(event.event_id)
        if previous is not None:
            if previous.model_dump(exclude={"ingested_at"}) != event.model_dump(
                exclude={"ingested_at"}
            ):
                raise EventIdentityConflictError(
                    f"Conflicting logical payload for event_id={event.event_id}"
                )
        else:
            unique[event.event_id] = event

    if not unique:
        raise ValueError("Refusing to compare against an empty event source")

    features = list(windows)
    if not features:
        raise ValueError("Refusing to compare an empty snapshot")
    if features != consolidate_windows(features):
        raise ValueError("Snapshot windows must be unique and sorted by user/window")

    plays: dict[str, list[datetime]] = defaultdict(list)
    for event in unique.values():
        if event.event_type == "play":
            plays[event.user_id].append(event.event_time)

    for timestamps in plays.values():
        timestamps.sort()

    mismatch_count = 0
    lower_count = 0
    higher_count = 0
    examples = []

    for feature in features:
        timestamps = plays.get(feature.user_id, [])
        start = feature.observation_time - timedelta(hours=1)
        expected = bisect_right(timestamps, feature.observation_time) - bisect_right(
            timestamps, start
        )

        if feature.play_count == expected:
            continue

        mismatch_count += 1
        lower_count += int(feature.play_count < expected)
        higher_count += int(feature.play_count > expected)

        if len(examples) < 10:
            examples.append(
                {
                    "user_id": feature.user_id,
                    "observation_time": feature.observation_time.isoformat(),
                    "window_start_exclusive": start.isoformat(),
                    "expected_source_count": expected,
                    "actual_snapshot_count": feature.play_count,
                }
            )

    return {
        "comparison_contract": "featureforge-stream.source-snapshot-comparison.v1",
        "source_deliveries": deliveries,
        "unique_source_events": len(unique),
        "duplicate_deliveries": deliveries - len(unique),
        "snapshot_windows_checked": len(features),
        "snapshot_users_checked": len({feature.user_id for feature in features}),
        "mismatch_count": mismatch_count,
        "snapshot_count_below_reference": lower_count,
        "snapshot_count_above_reference": higher_count,
        "mismatch_examples": examples,
        "passed": mismatch_count == 0,
        "scope": "event-time reconstruction for supplied snapshot keys",
        "missing_window_coverage_checked": False,
        "flink_admission_simulated": False,
        "historical_online_availability_checked": False,
    }


def compare_snapshot_with_coverage(
    events: Iterable[InteractionEventV1],
    windows: Iterable[UserPlayCountFeatureV1],
) -> dict:
    """Compare counts and the complete minute-aligned event-time window key set.

    All valid event types create windows in the current Flink job. This checks
    full event-time reconstruction, not watermark-dependent window admission.
    """
    source = list(events)
    features = list(windows)

    # Reuse complete identity, payload, and snapshot validation.
    report = compare_snapshot_with_events(source, features)

    expected_keys: set[tuple[str, datetime]] = set()
    for event in source:
        first_end = event.event_time.replace(second=0, microsecond=0)
        if event.event_time != first_end:
            first_end += timedelta(minutes=1)

        for minute in range(60):
            expected_keys.add((event.user_id, first_end + timedelta(minutes=minute)))

    actual_keys = {(feature.user_id, feature.observation_time) for feature in features}
    missing = sorted(expected_keys - actual_keys)
    unexpected = sorted(actual_keys - expected_keys)

    def examples(keys: list[tuple[str, datetime]]) -> list[dict]:
        return [
            {
                "user_id": user_id,
                "observation_time": observation.isoformat(),
            }
            for user_id, observation in keys[:10]
        ]

    return {
        **report,
        "comparison_contract": "featureforge-stream.source-snapshot-coverage.v1",
        "scope": "counts and complete window keys for full event-time reconstruction",
        "missing_window_coverage_checked": True,
        "expected_snapshot_windows": len(expected_keys),
        "missing_window_count": len(missing),
        "unexpected_window_count": len(unexpected),
        "missing_window_examples": examples(missing),
        "unexpected_window_examples": examples(unexpected),
        "passed": report["passed"] and not missing and not unexpected,
    }
