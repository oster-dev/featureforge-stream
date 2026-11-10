# FeatureForge Stream Transition

## Decision

FeatureForge Stream is frozen as a standalone streaming-engine prototype.

Future platform development continues in the main FeatureForge repository.

## Reason

The streaming prototype has proven a bounded set of streaming capabilities:

- Kafka event ingestion.
- Flink event-time processing.
- Redis online materialization.
- Historical Parquet snapshots.
- Late-event and duplicate handling.
- Source-to-snapshot reconciliation.
- Controlled recovery verification.

However, continuing it as a separate repository risks creating a second
feature platform with its own feature definitions, historical semantics,
and consumer interface.

## Consolidation approach

Selected streaming capabilities will be integrated into FeatureForge under
shared contracts.

FeatureForge will own:

- Feature and label contracts.
- Batch and streaming computation.
- Versioned historical storage.
- Online serving.
- A unified consumer interface.

FeatureForge Stream remains available as a documented prototype and
reference implementation.

## What is not being copied

This repository will not be copied wholesale into FeatureForge.

Instead, the following will be evaluated for integration:

- Event contracts.
- Deterministic reference implementations.
- Kafka/Flink streaming jobs.
- Redis materialization logic.
- Snapshot export logic.
- Reconciliation and recovery tests.

Components that duplicate platform responsibilities will not be migrated.

## Next step

Define the FeatureForge platform blueprint before migrating code.
