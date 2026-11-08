# ADR-001: Three Repositories, One Connected Data and ML System

- Status: Accepted
- Date: 2026-11-08
- Scope: Responsibilities and integration boundaries across Projects 1, 2, and 3

## Context

The portfolio consists of three separate repositories with complementary
responsibilities:

1. FeatureForge: the batch and feature-data foundation.
2. FeatureForge Stream: the streaming feature infrastructure.
3. An end-to-end ML workflow consuming the relevant outputs of the first
   two projects.

The objective is one coherent, reproducible system, not three unrelated
technology demonstrations.

Project 1 has already been connected to Project 2 through an explicit
adapter consuming existing FeatureForge source-event Parquet output.

Project 2 has demonstrated a source-to-online-store path:

```text
FeatureForge source-event Parquet
    → explicit event adapter
    → Kafka
    → PyFlink validation
    → PyFlink feature computation
    → Kafka feature updates
    → guarded Redis materialization
    → validated lookup
```

Project 3 is planned but not yet integrated. Its detailed input contracts
must not be described as implemented.

The original Project 2 plan is a planning input, not an immutable checklist.
Scope may change when an item duplicates an existing responsibility, does
not support the connected system, or introduces unjustified complexity.

## Decision

Maintain three independently understandable repositories connected through
explicit, versioned data and interface contracts.

Keep the architectural purpose stable while allowing implementation details
and project scope to evolve through documented decisions.

### Project 1: Batch and Feature-Data Foundation

Project 1 owns its existing responsibilities, including:

- Source-data generation and validation.
- Historical batch feature computation.
- Backfills and canonical offline feature storage.
- Historical retrieval.
- Existing materialization, serving, quality, and freshness contracts.

Project 2 must not copy or rebuild these capabilities merely to make its
repository appear self-contained.

Changes to Project 1 should be limited to justified bug fixes or deliberate,
compatible integration requirements.

### Project 2: Streaming Feature Infrastructure

Project 2 owns:

- Explicit adaptation of supported Project 1 source-event outputs.
- Streaming input validation and error routing.
- Event identity and deduplication semantics.
- Event-time assignment, windows, watermarks, and late-event handling.
- Stateful streaming feature computation.
- Online update ordering and materialization.
- Declared streaming output contracts.
- Streaming recovery, replay, freshness, and operational evidence.

Project 2 must provide outputs that a downstream consumer can understand
without relying on this project's internal execution details.

Any future offline output must have an explicit schema, time semantics,
versioning policy, and declared relationship to Project 1's offline contracts.
Compatibility must be tested rather than assumed.

### Project 3: End-to-End ML Workflow

Project 3 will own the integrated ML workflow, including the planned
orchestration, experiment tracking, model lifecycle, and deployment
demonstration.

It should consume the relevant artifacts and interfaces of Projects 1 and 2
instead of reimplementing their feature pipelines.

Exact feature selection, training inputs, label inputs, and inference
interfaces remain to be specified and tested.

## Data Flow

The intended system is not restricted to one linear dependency chain:

```text
Project 1
    ├── Historical data and batch features ───────────┐
    │                                                │
    └── Source-event output                          │
            ↓                                        │
        Project 2                                    │
            ├── Streaming online feature interface ──┤
            └── Declared offline output, planned ────┤
                                                     ↓
                                                 Project 3
                                                 ML workflow
```

Project 3 may consume Project 1 outputs directly where appropriate.
Project 2 must not become an unnecessary intermediary for existing
historical data.

This diagram expresses intended responsibilities. It does not claim that
all depicted connections currently exist.

## Current Integration Boundary

The implemented Project 1 → Project 2 connection consumes source events,
not Project 1's finished batch feature tables.

The existing adapter:

- Accepts an explicit source-file path.
- Maps supported source fields into the streaming event contract.
- Preserves declared historical event and ingestion timestamps.
- Uses dataset identity when deriving streaming event identifiers.
- Explicitly excludes unsupported source event types.
- Does not import Project 1 application internals.

The observed adapted replay contained 3,424 records representing 3,424
distinct event IDs.

The manual Redis/reference check covered 436 users with zero count
mismatches at the stored observation timestamps.

These results do not establish complete cross-project feature parity,
latest-window coverage, recovery, or production readiness.

## Integration Contract Requirements

Every cross-repository handoff must declare:

- The producing and consuming repository.
- Artifact location or access interface.
- Schema and version.
- Entity identifiers and their meaning.
- Feature definitions and supported event types.
- Event time, observation time, and any availability-time semantics.
- Update, duplicate, and replay behavior.
- Missing-data, stale-data, and incomplete-data behavior.
- Compatibility and migration rules.
- A reproducible test of the handoff.

A schema version alone is not a complete compatibility policy.

## Semantic Boundaries

Shared source data does not imply identical features.

Batch and streaming results may only be compared when their feature
definitions, entity sets, time boundaries, and treatment of late data
are aligned.

A historical event-time reconstruction is not automatically a reconstruction
of what was available to an online consumer at that historical time.

Using Redis in multiple projects does not establish compatible storage
formats. Project 2's current JSON key format must not be treated as
Feast-compatible without an explicit, tested integration.

Reusing an existing implementation does not require a new shared package.
Introduce shared code only when a demonstrated need justifies its coupling
and maintenance cost.

## Scope and Complexity Rules

An implementation item belongs in Project 2 only when it:

1. Supports a necessary integration boundary.
2. Establishes or verifies a streaming correctness guarantee.
3. Enables required operational behavior.
4. Provides a concrete downstream usability benefit.

Do not add components solely because they appeared in an earlier plan or
are mentioned in a target job description.

Additional features should demonstrate distinct required semantics or serve
a downstream use case, not merely increase the feature count.

A catalog, specification interface, notebook, dashboard, or new service is
not automatically required. Its scope and repository ownership must be
justified.

ML workflow orchestration and model lifecycle management belong in
Project 3 rather than being duplicated in Project 2.

Scope changes must be explicit. Removing an original requirement must not
be represented as having implemented it.

## Verification

Integration success requires executable evidence, not only an architecture
diagram or successful process startup.

The next verification milestone is a version-controlled acceptance path
for the existing Project 1 → Project 2 flow that:

- Uses an explicit, reproducible input selection.
- Establishes isolated replay topics and Redis keys.
- Configures retention before writing historical replay outputs.
- Waits for successful bounded job completion.
- Materializes the completed feature output.
- Checks expected users, observation windows, and feature values.
- Produces a machine-readable result.
- Fails visibly on missing data or mismatches.

Recovery and downstream Project 3 integration require separate tests.
One successful replay must not be described as proof of those behaviors.

## Consequences

### Benefits

- Clear ownership across repositories.
- Reuse of the completed batch foundation.
- Streaming work remains focused on its actual responsibilities.
- Project 3 can demonstrate consumption rather than duplicate infrastructure.
- Scope decisions are tied to system value rather than tool count.
- Claims can be traced to concrete integration and correctness evidence.

### Costs and Limitations

- Cross-repository schema and semantic changes require coordination.
- Separate repositories require explicit setup and version information.
- Some existing plans or documentation may need revision.
- Intended interfaces remain proposals until implemented and tested.

## Review Policy

Review existing files against this decision incrementally.

Keep working, tested code when it already satisfies the architecture.
Change files only for a concrete responsibility, contract, correctness,
or usability issue.

Do not perform a wholesale rewrite merely to match a new directory layout
or terminology.