"""Reconcile snapshot differences with explicitly observed late-output records.

This explains output relative to recorded exclusions. It does not independently
prove watermark decisions or simulate partial admission to sliding windows.
"""

from collections.abc import Iterable

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.online_store import UserPlayCountFeatureV1
from featureforge_stream.reference.snapshot_parity import compare_snapshot_with_coverage


def reconcile_late_output(
    events: Iterable[InteractionEventV1],
    windows: Iterable[UserPlayCountFeatureV1],
    late_records: Iterable[dict],
) -> dict:
    """Return unfiltered and observed-exclusion comparisons side by side.

    Duplicate late event IDs are rejected rather than interpreted as independent
    admission decisions. Exclusion is by logical event ID for this bounded
    comparison, not a general model of processing-time TTL or per-window admission.
    """
    source = list(events)
    features = list(windows)

    # Validate all source identities and preserve the original comparison.
    full_comparison = compare_snapshot_with_coverage(source, features)

    source_payloads = {event.model_dump_json() for event in source}
    excluded = {}
    late_delivery_count = 0

    for record in late_records:
        if not isinstance(record, dict):
            raise TypeError("late_records must contain dictionaries")
        if (
            record.get("error_schema_version") != "v1"
            or record.get("feature_name") != "user_play_count_1h"
            or record.get("reason_code") != "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS"
        ):
            raise ValueError("Unsupported late-output envelope")

        event = InteractionEventV1.model_validate_json(record["raw_payload"])
        if event.model_dump_json() not in source_payloads:
            raise ValueError("Late-output payload does not match a source delivery")
        if event.event_id in excluded:
            raise ValueError("Duplicate late-output event ID")

        excluded[event.event_id] = event
        late_delivery_count += 1

    remaining = [event for event in source if event.event_id not in excluded]
    if not remaining:
        raise ValueError("No source events remain after observed exclusions")

    adjusted_comparison = compare_snapshot_with_coverage(remaining, features)

    return {
        "reconciliation_contract": "featureforge-stream.observed-late-reconciliation.v1",
        "full_input_comparison": full_comparison,
        "observed_late_output": {
            "records_validated": late_delivery_count,
            "unique_excluded_event_ids": len(excluded),
            "excluded_source_deliveries": len(source) - len(remaining),
            "events": [
                {
                    "event_id": str(event.event_id),
                    "user_id": event.user_id,
                    "event_type": event.event_type,
                    "event_time": event.event_time.isoformat(),
                }
                for event in sorted(excluded.values(), key=lambda event: str(event.event_id))
            ],
        },
        "comparison_after_observed_exclusions": adjusted_comparison,
        "consistent_with_observed_exclusions": adjusted_comparison["passed"],
        "watermark_decisions_independently_verified": False,
        "partial_window_admission_simulated": False,
        "scope": (
            "Full reconstruction and reconstruction excluding verified late-output "
            "event IDs; not independent proof of late-event admission correctness"
        ),
    }
