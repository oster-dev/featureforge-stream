# User Play Count 1h — Stream Snapshot V1

## Purpose and ownership

FeatureForge Stream exports retained streaming feature windows as a bounded,
run-scoped Parquet artifact.

This is not FeatureForge's canonical offline feature store. It does not replace
Project 1 backfills, historical retrieval, quality gates, or Feast FileSources.
Project 1 remains unchanged.

The snapshot is not schema-compatible with Project 1's existing engagement
feature tables: those contain multi-feature records with a positive window_days
field, while this artifact represents the streaming one-hour play-count feature.

A downstream integration requires an explicit, tested adapter or consumer.
No Project 3 integration is claimed.

## Artifacts

A completed export directory contains:

- windows.parquet: consolidated historical streaming feature windows.
- verification.json: source offsets, artifact hash, row counts, and Redis
  comparison results.

Contract identifiers:

- Parquet: featureforge-stream.user-play-count-1h.snapshot.v1
- Export report: featureforge-stream.replay-snapshot-export.v1

## Row schema

| Field | Arrow type | Meaning |
|---|---|---|
| feature_schema_version | string, non-null | Feature payload version, v1 |
| feature_name | string, non-null | user_play_count_1h |
| user_id | string, non-null | Synthetic user identifier |
| play_count | int64, non-null | Non-negative play count |
| observation_time | timestamp[us, UTC], non-null | Inclusive window end |
| window_start_exclusive | timestamp[us, UTC], non-null | Exclusive window start |

The feature window is exactly one hour:

    (window_start_exclusive, observation_time]

The logical row key is (user_id, observation_time). Rows are sorted by that key.

For repeated updates to the same key, the maximum play_count is retained.
This policy applies only to the existing append-only count feature without
retractions. It cannot repair an inflated count or represent downward revisions.

Different observation times remain separate rows. A newer window with a zero
count does not remove an older window.

## Time and completeness semantics

The artifact represents event-time reconstruction from retained feature output.
It does not reconstruct what an online consumer could have known at a historical
observation time.

No availability timestamp, point-in-time training safety, or source-data
completeness is inferred from observation_time.

The exported rows cover windows present in the captured Kafka output. The
exporter does not invent missing users or missing windows, and does not
independently verify the Flink computation against source events.

## Export prerequisites

- Replay processing and Redis materialization have finished.
- No concurrent writers target the Kafka topic or Redis namespace.
- The Kafka topic has exactly one partition.
- The retained range is non-empty and starts at offset zero.
- Record offsets are contiguous; tombstones are unsupported.
- Kafka low/high watermarks remain unchanged during the Kafka read.
- The output directory does not already exist.

The exporter records the topic, partition, inclusive start offset, exclusive
end offset, and number of records read. This identifies the range within the
declared local run; it is not a globally unique Kafka cluster/topic identity.

Auto-commit, automatic offset storage, and automatic topic creation are disabled.
Redis feature values are read, not modified.

## Verification

The exporter:

1. Validates Kafka feature payloads.
2. Consolidates updates per user and observation time.
3. Writes windows.parquet.
4. Reads and validates the persisted Parquet artifact.
5. Selects the newest window per user from the restored rows.
6. Compares complete feature values with Redis and checks for additional
   namespace keys.
7. Writes verification.json, including the Parquet SHA-256.

For an accepted export, verification.json must exist, its snapshot hash must
match windows.parquet, and comparison.passed must be true.

The comparison requires a quiescent namespace. Redis reads and the database
scan are not an atomic snapshot.

This proves persisted snapshot-to-online consistency for the checked run.
It does not establish independent batch-stream parity, checkpoint recovery,
Feast compatibility, or a deployed downstream training workflow.

## Failure and publication boundaries

The implementation holds bounded replay data in memory. It is not a continuous
offline sink or a large-scale dataset writer.

Existing output directories and snapshot files are not overwritten.

The Parquet artifact and report are not published as one crash-atomic unit.
An interrupted or failed export may leave an incomplete directory. A directory
without a valid matching report is not an accepted export.

A mismatch writes a failed comparison report and exits unsuccessfully.
Other failures may occur before a report can be written.

## Versioning

Readers require the declared Arrow schema and contract metadata.
Incompatible schema or semantic changes require a new contract version and
an explicit reader or migration decision.

## Implementation and tests

- src/featureforge_stream/snapshot.py
- scripts/export_replay_snapshot.py
- tests/unit/test_snapshot.py
- tests/unit/test_export_replay_snapshot.py
