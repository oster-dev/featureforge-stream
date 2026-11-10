# Architecture — FeatureForge Stream

FeatureForge Stream is a local, production-inspired streaming feature engine
built with Kafka, Apache Flink, and Redis.

It consumes interaction events from Kafka, validates their contracts, computes
event-time features with Apache Flink, publishes feature updates back to Kafka,
and materializes online feature values into Redis.

## Project status

This repository is frozen as a standalone streaming-engine prototype.

Its proven streaming capabilities will be selectively consolidated into the
main FeatureForge repository, which is evolving into a unified ML feature
platform.

FeatureForge will own:

- Feature and label contracts.
- Batch and streaming computation.
- Versioned historical storage.
- Low-latency online serving.
- A unified access interface for ML consumers.

FeatureForge Stream will not continue as a second feature platform.

See [docs/TRANSITION.md](docs/TRANSITION.md).

## Implemented features

| Feature | Window | Output |
|---|---|---|
| `user_play_count_1h` | Trailing 1 hour | Unique play count |
| `user_skip_rate_24h` | Trailing 24 hours | Play count, skip count, and skip rate |

Both features use event time, not Kafka arrival time.

### user_play_count_1h

Counts unique `play` events for one user within:

```text
(observation_time - 1 hour, observation_time]
```

Only `play` events increment the count.

### user_skip_rate_24h

For one user within:

```text
(observation_time - 24 hours, observation_time]
```

computes:

```text
skip_rate = skip_count / (play_count + skip_count)
```

Rules:

- Counts use unique logical events.
- A zero denominator returns `0.0`.
- The rate is a float between `0.0` and `1.0`.
- No session-level pairing between plays and skips is inferred.
- Late corrections can increase or decrease the rate.

## System overview

```text
FeatureForge Parquet source
        |
        v
Replay adapter
        |
        v
Raw interaction-event Kafka topic
        |
        v
Validation job: Kafka -> Flink -> Kafka
        |
        v
Feature jobs: Kafka -> Flink -> Kafka
        |
        v
Materializers: Kafka -> Redis
        |
        v
Online lookup
```

The implemented skip-rate proof covers:

```text
Flink feature-job input -> Kafka feature output -> Materializer -> Redis
```

It starts at the feature-job input. It does not include upstream validation,
Project 1 source integration, downstream ML consumption, skip-rate snapshots,
or checkpoint recovery.

## Core design principles

- Event time defines feature-window membership.
- Contracts are validated explicitly.
- Logical identity and receipt metadata are separate concepts.
- Reference correctness and distributed execution are tested separately.
- Connected components require joined integration evidence.
- Late corrections are part of normal feature computation.
- Duplicate suppression has a finite, documented retention horizon.
- Online update rules are feature-specific.
- Independent runs must not silently share incompatible state.
- Every claim is bounded by its actual test or recorded drill.

## Runtime components

| Component | Responsibility |
|---|---|
| Kafka | Event transport, retained input/output, and stage decoupling |
| Validation job | Validates raw events and separates valid records from rejections |
| Play-count job | Computes one-hour user play counts |
| Skip-rate job | Computes 24-hour user play and skip counts and their ratio |
| Play-count materializer | Applies guarded play-count updates to Redis |
| Skip-rate materializer | Applies guarded skip-rate count-vector updates to Redis |
| Redis | Stores the latest accepted observation window per user and feature namespace |
| Replay adapter | Reads supported Project 1 Parquet events and publishes adapted records |
| Snapshot exporter | Creates a bounded play-count Parquet artifact and verification report |
| Source verifier and reconciler | Compare declared source-derived expectations with snapshots and recorded late output |

## Evidence status

| Capability | Current evidence | Boundary |
|---|---|---|
| Event validation | Contract and Flink validation tests | Explicit V1 event contract |
| Play-count computation | Python reference and Flink integration tests | One-hour event-time feature |
| Skip-rate computation | Python reference and eight isolated Flink cases | 24-hour feature, boundaries, late corrections, duplicates, zero denominator, and late routing |
| Play-count materialization | Kafka/Redis verification and Redis tests | Single-partition append-only count semantics |
| Skip-rate Redis store | Policy tests and twelve Redis integration cases | Componentwise count ordering within a consistent run namespace |
| Skip-rate materialization | Unit tests and a real Kafka-to-Redis integration test | Controlled feature payloads published directly to Kafka |
| Combined skip-rate path | Isolated integration test using actual Flink output, late corrections, duplicates, and output replay | Starts at feature-job input |
| Play-count snapshots | Versioned Parquet export and snapshot-to-online verification | Bounded, run-scoped artifact |
| Source reconciliation | Source-derived checks and verified late output | Declared play-count replay scope |
| TaskManager recovery | Documented manual play-count recovery drill | Specific local failure scenario |

The presence of a component does not imply that every possible combination of
components has been tested together.

## Source integration

FeatureForge Stream extends FeatureForge Batch through explicit contracts.
It does not rebuild or modify Project 1's canonical offline store.

The replay adapter reads supported Parquet source records and maps them to
`InteractionEventV1`.

Currently supported source event types are:

- `play`
- `impression`

The streaming event contract also supports `skip` and `complete`.
Support in the stream contract does not mean the existing Project 1 adapter
produces every event type.

Historical publication requires a dedicated topic beginning with:

```text
interaction-events.replay.
```

The adapter refuses to publish historical records directly to the normal live
input topic.

## Event contract

Input contract: `interaction-events.v1`.

```json
{
  "event_id": "00000000-0000-4000-8000-000000000001",
  "event_time": "2026-01-01T11:00:00Z",
  "ingested_at": "2026-01-01T11:00:01Z",
  "user_id": "user_000001",
  "content_id": "content_000001",
  "event_type": "play",
  "play_seconds": 0,
  "source": "web",
  "schema_version": "v1"
}
```

Validation rules:

- `event_id` must be a UUID.
- Timestamps must be timezone-aware and are normalized to UTC.
- `ingested_at >= event_time`.
- Entity IDs must be non-empty.
- `event_type` must be `impression`, `play`, `skip`, or `complete`.
- `play_seconds` must be a non-negative integer.
- `source` must be `web`, `mobile`, or `tv`.
- `schema_version` must be `v1`.
- Unknown fields are rejected.

Detailed contracts:

- [Interaction Events V1](docs/contracts/interaction-events-v1.md)
- [User Play Count 1h V1](docs/contracts/user-play-count-1h-v1.md)
- [User Skip Rate 24h V1](docs/contracts/user-skip-rate-24h-v1.md)

## Time semantics

Window membership uses `event_time`.

`ingested_at` remains separate receipt metadata. The Python references can
include an event whose receipt timestamp is later than the observation time.

These references reconstruct event-time values from supplied records.
They do not establish what an online consumer could have known at a
historical observation time.

Both features use right-closed windows:

```text
(observation_time - window_length, observation_time]
```

| Feature | Window | Slide |
|---|---|---|
| `user_play_count_1h` | 1 hour | 1 minute |
| `user_skip_rate_24h` | 24 hours | 1 minute |

Current configuration:

```text
Bounded out-of-orderness: 10 minutes
Allowed lateness: 30 minutes
```

With sliding windows, an event can be too late for one assigned window while
still being accepted by another. Only an event too late for all assigned
windows is sent to the late output.

## Late corrections

Play counts can increase when an additional play is accepted for an already
emitted window.

Skip rates can move in either direction:

| Accepted same-window state | Plays | Skips | Rate |
|---|---:|---:|---:|
| Initial | 1 | 1 | 0.5 |
| Additional play | 2 | 1 | 1/3 |
| Additional skip | 2 | 2 | 0.5 |

A lower rate is not necessarily a stale update.

## Duplicate handling

Logical identity is `event_id`.

Repeated deliveries with the same logical payload are suppressed while
deduplication state is retained. `ingested_at` is excluded from logical
identity because it is receipt metadata, not business meaning.

| Feature job | Deduplication TTL |
|---|---|
| Play count | 24 hours |
| Skip rate | 48 hours |

This is finite-horizon deduplication, not unlimited replay protection.

## Redis write policies

Redis stores the latest accepted observation window per user in a
feature-specific namespace. These are latest-value stores, not historical
window stores.

### Play count

| Incoming feature | Decision |
|---|---|
| Newer observation time | `written` |
| Older observation time | `stale` |
| Same time, larger count | `written` |
| Same time, equal count | `unchanged` |
| Same time, smaller count | `stale` |

This supports append-only count corrections. It does not support retractions
or repair an inflated count.

### Skip rate

Within one observation window, compare `(play_count, skip_count)`.

| Incoming feature | Decision |
|---|---|
| Newer observation time | `written` |
| Older observation time | `stale` |
| Same time, identical counts | `unchanged` |
| Both counts at least as large, one larger | `written` |
| Both counts at most as large, one smaller | `stale` |
| One count increases and the other decreases | Raise `SkipRateUpdateConflictError` |

The policy does not compare rates by magnitude. It does not merge conflicting
records by taking componentwise maxima, because that could manufacture a result
the job never emitted.

Both stores use Redis optimistic concurrency:

1. Watch the user key.
2. Read and validate its current payload.
3. Apply the feature-specific policy.
4. For an accepted write, execute `MULTI/SET/EXEC`.
5. Retry after `WatchError`, reading the current value again.

Retries are bounded at 16 attempts.

## Materializer boundaries

Materializers currently support exactly one Kafka partition.

In bounded mode, they capture the exclusive high offset at startup and resume
from the group's committed offset or retained low offset.

Behavior:

- An empty readable range is rejected.
- An already caught-up group returns without writes.
- Tombstones and invalid feature payloads are rejected.
- Redis processing precedes offset commit.
- A failed record is not committed as successfully processed.

A Redis write and a Kafka offset commit are not one distributed transaction.
A failure between them can cause redelivery.

## Historical replay

For an isolated staged replay:

1. Create dedicated topics.
2. Publish the complete adapted dataset.
3. Run validation to `FINISHED`.
4. Verify validated and rejected output.
5. Run the selected feature job to `FINISHED`.
6. Verify feature and late output.
7. Materialize into an isolated Redis namespace.
8. Run the applicable consistency checks.

Starting a bounded downstream stage too early can capture an incomplete
upstream range.

Fresh topics and namespaces do not by themselves establish deterministic
late-event admission; watermark scheduling remains relevant.

Replaying retained feature output through a materializer is not the same
operation as recomputing that output from source events.

## Snapshots

Snapshot support currently applies to `user_play_count_1h`.

A completed export contains:

```text
windows.parquet
verification.json
```

The snapshot is not Project 1's canonical offline store and is not
schema-compatible with its existing engagement feature tables.

Rows are keyed by `(user_id, observation_time)`. Repeated same-window updates
are consolidated using maximum `play_count`.

This is valid only for the declared append-only count model. It cannot
represent downward revisions or repair inflated counts.

Skip-rate snapshot support is not implemented.

## Checkpoint recovery

Both feature jobs enable checkpointing at a 10-second interval.

Checkpoint configuration alone does not prove successful recovery or
end-to-end exactly-once processing.

A controlled local play-count drill:

1. Kept the JobManager and Kafka running.
2. Killed and restarted the TaskManager.
3. Observed the same job restore from checkpoint `23`.
4. Repeated an original play and added a new play.
5. Observed the reference count `2`.
6. Observed no excessive count during the following 15 seconds.

This is manual evidence for a specific TaskManager-failure scenario.

See
[TaskManager Checkpoint Recovery](docs/runbooks/taskmanager-checkpoint-recovery.md).

## Local infrastructure

Docker Compose provides:

- Kafka in KRaft mode
- Flink JobManager
- Flink TaskManager
- Redis with append-only persistence enabled

| Service | Endpoint |
|---|---|
| Kafka | `127.0.0.1:9092` |
| Flink UI / REST | `127.0.0.1:8081` |
| Redis | `127.0.0.1:6379` |

Kafka's internal container endpoint is `kafka:29092`.

The TaskManager has two task slots. Feature jobs use parallelism one.

This is a local development stack, not a hardened production deployment.
Kafka uses plaintext listeners and does not establish production
authentication, authorization, encryption, or secret management.

## Testing

Run default checks:

```bash
make check
```

Run isolated skip-rate Flink tests:

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_user_skip_rate_job.py \
  -v -s -x
```

Run skip-rate Redis tests:

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_redis_skip_rate_store.py \
  -v -x
```

Run the combined skip-rate feature-job-to-Redis test:

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_skip_rate_flink_to_redis.py \
  -v -s -x
```

The combined test verifies:

1. Dedicated input, feature-output, and late-output topics.
2. The actual skip-rate Flink job.
3. Actual Flink output in Kafka.
4. The real bounded materializer.
5. Redis values, counts, rate, and window metadata.
6. Late corrections and duplicate delivery.
7. Replay through a fresh materializer consumer group.

Verified states:

| Phase | Plays | Skips | Rate |
|---|---:|---:|---:|
| Initial output | 1 | 1 | 0.5 |
| Additional allowed-late play | 2 | 1 | 1/3 |
| Additional allowed-late skip | 2 | 2 | 0.5 |
| Repeated events plus a distinct play | 3 | 2 | 0.4 |

## Correctness boundaries

Supported within the demonstrated scope:

- Validated event contracts
- Right-closed event-time windows
- Finite-horizon logical deduplication
- Tested late corrections
- Explicit all-windows-too-late routing
- Feature-specific guarded Redis writes
- Single-partition materialization
- A verified skip-rate feature-job-to-Redis path using actual Flink output
- Replay of retained skip-rate output without changing the final Redis value
- Bounded staged replay
- Play-count snapshots and declared consistency checks
- A documented manual TaskManager recovery scenario

Not claimed:

- End-to-end exactly-once processing
- Unlimited duplicate or replay protection
- General retractions or arbitrary feature revisions
- Repair of inflated counts
- Equivalence of independent runs sharing one namespace
- Complete Spark/Flink parity
- Historical online availability or leakage-safe training retrieval
- Canonical Project 1 offline-store compatibility
- Skip-rate snapshots or skip-rate recovery
- A single joined upstream-validation-to-Redis proof
- A complete three-repository integration proof
- Multi-partition materialization
- Production-scale performance or high availability
- Deployed monitoring, schema-registry governance, or AWS production operation
- A deployed downstream ML workflow

## Transition boundary

This repository remains a valuable reference implementation.

Its streaming capabilities will be selectively integrated into FeatureForge
under shared feature, time, storage, and consumer contracts.

It will not continue as a second feature platform.

## Guiding rule

> Define the contract, define the reference, test the semantics,
> then connect and verify the infrastructure.