# Source-to-Snapshot Verification and Observed Late Reconciliation V1

## Purpose

Compare a persisted streaming snapshot with independent event-time reconstruction
from an explicitly selected validated Kafka input.

Two results remain distinct:

1. Full input reconstruction: compare counts and complete expected window keys.
2. Observed late reconciliation: repeat reconstruction after excluding event IDs
   whose complete payloads are verified against the corresponding late output.

A successful reconciliation does not overwrite a failed full-input comparison.

## Ownership and scope

This verification belongs to FeatureForge Stream. It does not modify Project 1,
rebuild its canonical offline store, or claim compatibility with its batch tables.

The comparison starts at validated streaming input. Original Parquet adaptation
and validation-stage completeness are outside this check.

It uses a Python event-time reference, not a Spark batch execution.

## Full input reconstruction

Logical events are deduplicated by event_id. Conflicting logical payloads fail
the comparison. ingested_at is excluded from logical identity.

Counts use:

    observation_time - 1 hour < event_time <= observation_time

Only play events increase the count.

The current Flink job assigns all valid event types to one-hour windows with a
one-minute slide. Expected window keys are therefore derived from all valid
input event types, including events that create zero-count windows.

For each event, the first observation is the event time rounded up to the next
whole minute, leaving exact-minute timestamps unchanged. The event belongs to
that observation and the following 59 minute-aligned observations.

The checker compares:

- Counts for every supplied snapshot row.
- Missing expected user/observation keys.
- Unexpected snapshot user/observation keys.

Receipt time does not filter event-time reconstruction.

## Observed late reconciliation

Late envelopes must declare:

- error_schema_version: v1
- feature_name: user_play_count_1h
- reason_code: TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS

Each decoded raw_payload must exactly match a normalized source delivery,
including ingested_at. Unknown or altered payloads are rejected.

Duplicate late event IDs are rejected. Verified event IDs are excluded from the
second reconstruction; both comparison results remain in the report.

This is an event-ID exclusion model for a bounded run. It does not simulate
partial admission to overlapping windows or processing-time deduplication TTL.
Excluding every source event is currently unsupported and rejected.

## Artifact integrity and lineage

The checker validates:

- The original export report contract and successful comparison status.
- The expected snapshot filename.
- The Parquet SHA-256 against the export report.
- The declared snapshot row counts.
- The snapshot schema, values, uniqueness, and ordering.

Kafka reads require complete, contiguous single-partition ranges starting at
offset zero. Input must be non-empty; late output may be empty. Each range must
remain unchanged during its own read.

Reports include topic names, partition and offset boundaries, normalized input
or late-envelope hashes where applicable, and the snapshot hash.

The caller must establish that input, late output, and feature output belong to
the same completed run. The checker does not independently prove that association.
Hashes identify the inspected content; they do not authenticate its producer.

Run without concurrent writers. Reads of separate topics and files are not a
cross-component atomic snapshot.

## Commands

Full reconstruction:

```bash
.venv/bin/python scripts/verify_source_snapshot.py \
  --input-topic "<validated-replay-topic>" \
  --snapshot-dir "<existing-snapshot-directory>" \
  --report "<new-report-path>.json"
```

Observed late reconciliation:

```bash
.venv/bin/python scripts/reconcile_source_snapshot.py \
  --input-topic "<validated-replay-topic>" \
  --late-topic "<matching-feature-run-late-topic>" \
  --snapshot-dir "<existing-snapshot-directory>" \
  --report "<new-reconciliation-report-path>.json"
```

Existing report paths are not overwritten. Reports are local artifacts and are
not crash-atomically published.

## Status interpretation

verify_source_snapshot.py exits unsuccessfully when counts or complete window
coverage differ from full input reconstruction.

reconcile_source_snapshot.py exits successfully when the snapshot matches
reconstruction after verified observed exclusions. Its RECONCILED status does
not imply that full-input reconstruction passed.

Neither command independently verifies actual watermark decisions, historical
online availability, checkpoint recovery, or end-to-end exactly-once behavior.

## Observed replay evidence — November 9, 2026

Run: ff-online-20261108T130402Z, retention-fix feature output.

| Check | Result |
|---|---:|
| Validated input deliveries / unique events | 3,424 / 3,424 |
| Snapshot windows / users | 200,399 / 436 |
| Full-input expected windows | 200,639 |
| Count mismatches on existing windows | 0 |
| Missing / unexpected windows against full input | 240 / 0 |
| Verified late-output events | 4 |
| Late event types | 2 plays, 2 impressions |
| Missing windows with reference count 1 / 0 | 120 / 120 |
| Remaining events after observed exclusions | 3,420 |
| Expected windows after observed exclusions | 200,399 |
| Count mismatches / missing / unexpected after exclusions | 0 / 0 / 0 |

The four observed exclusions explain the entire difference for this run.
Their watermark-based rejection decisions were not independently reconstructed.

## Implementation

- src/featureforge_stream/reference/snapshot_parity.py
- src/featureforge_stream/reference/reconciliation.py
- scripts/verify_source_snapshot.py
- scripts/reconcile_source_snapshot.py

Tests cover reference counts, boundaries, duplicate identity, complete window
coverage, strict exclusion validation, Kafka read failures, and artifact checks.
