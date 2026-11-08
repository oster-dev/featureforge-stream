# FeatureForge Stream

A production-inspired local reference implementation for near-real-time
feature computation.

FeatureForge Stream extends the batch-first foundation of
[FeatureForge](https://github.com/oster-dev/featureforge) with stateful,
event-time streaming and explicit contracts for event identity, time windows,
late arrivals, replay, and online/offline consistency.

The first connected source-to-online-store path is implemented:

```text
Existing FeatureForge Parquet events
    → contract adapter and Kafka producer
    → PyFlink validation
    → PyFlink user_play_count_1h computation
    → Kafka feature updates
    → guarded Redis materialization
    → validated per-user lookup
```

FeatureForge v0.1.0 remains unchanged. This repository consumes its existing
source-event output rather than rebuilding its generator or batch platform.

## Current Status

Implemented and locally verified:

- Installable Python package with a `src` layout.
- Validated, immutable interaction-event model.
- Deterministic feature reference and golden fixture.
- Arrival-order admission reference and boundary tests.
- Docker Compose stack with Kafka, Flink, and Redis.
- Python workers executing application code on the TaskManager.
- Validation routing into separate valid and rejected Kafka topics.
- Stateful deduplication by event ID with bounded retention.
- Event-time, minute-aligned rolling-hour feature computation.
- Flink checkpoint creation.
- Kafka feature output and a separate late-event output.
- Integration coverage for golden values, late corrections, duplicate
  delivery, and too-late routing.
- A validated Redis store for append-only user play counts.
- Transactional Redis stale-write protection using optimistic locking.
- A controlled Redis concurrent-write and retry integration test.
- A Parquet adapter for existing FeatureForge source events.
- Configurable job topics, consumer groups, and job names.
- Bounded historical replay using the existing streaming job logic.
- A completed cross-project replay with source-verified example values.
- Kafka-to-Redis materialization with explicit offset commits.
- Bounded materialization using a captured Kafka end offset.
- Eleven infrastructure-free materializer unit tests.
- A completed Redis/reference comparison for all 436 users in the replay input.

The completed materialization connects Kafka feature updates to the existing
guarded Redis store. A read-only reference check found no missing users,
unexpected users, or count mismatches at the stored observation timestamps.

This is not a claim of full batch-stream parity, complete latest-window
coverage, crash recovery, or end-to-end exactly-once processing.

### Latest Verified Results

```text
Ruff lint: passed
Ruff format: passed
Default test run: 131 passed, 14 deselected
git diff --check: passed before this documentation update
Previous targeted Redis integration run: 10 passed
FeatureForge source adaptation: 3,424 records
Replay validation: FINISHED
Latest replay feature computation: FINISHED
Previous source-verified feature examples: 5/5 matched
Kafka-to-Redis materialization: completed
Materializer updates processed: 321,205
Final committed materializer lag: 0
Redis/reference comparison: 436 users checked, 0 mismatches
```

The complete 14-case integration selection was not rerun during the latest
materializer verification.

The 14 integration cases comprise:

- Four Kafka/Flink integration cases.
- Ten Redis integration cases.

They are excluded from the default test run.

The eleven new materializer unit tests use controlled doubles. They are not
additional live Kafka/Redis integration cases.

## Relationship to FeatureForge

FeatureForge v0.1.0 provides the batch-first foundation:

- Synthetic source-data generation.
- Historical feature computation.
- Canonical offline storage.
- Data-quality and freshness checks.
- Feast historical retrieval.
- Online materialization and serving.

FeatureForge Stream adds a continuous event-processing path. It does not
reimplement the existing generator, batch pipeline, or Feast platform.

The repositories remain separate:

```text
featureforge/
└── output/
    └── source_data/
        └── events.parquet
                │
                │ explicit file input
                ▼
featureforge-stream/
└── replay adapter
    → Kafka
    → PyFlink validation
    → PyFlink feature computation
    → Kafka feature updates
    → Redis materializer
    → online lookup
```

The adapter receives a source-file path. It does not import FeatureForge's
application internals or require copying its implementation into this
repository.

The current streaming feature is independently defined. Reading the
FeatureForge source events does not modify existing Feast feature views,
offline feature partitions, or the released FeatureForge repository.

A historical file replay is not a continuously emitting live source. It
exercises the streaming computation with existing source events.

## Architecture

```text
FeatureForge source-event Parquet / integration fixture
                         ↓
              Event adapter / producer
                         ↓
                 Raw Kafka events
                         ↓
            PyFlink validation and routing
                  ┌──────┴──────┐
                  ↓             ↓
          Validated events   Rejected events
                  ↓
         PyFlink user_play_count_1h
                  ↓
         Event-time timestamp assignment
                  ↓
         Deduplication keyed by event_id
                  ↓
         Sliding windows keyed by user_id
                  ├─────────────────────┐
                  ↓                     ↓
          Kafka feature updates    Late-event output
                  ↓
          Kafka-to-Redis materializer
                  ↓
          Guarded Redis online store
                  ↓
          Validated per-user lookup
```

### Local Components

| Component | Current implementation |
|---|---|
| Kafka | Apache Kafka 3.9.1, single local broker/controller |
| Flink | Apache Flink 1.20.3 |
| JobManager | One container |
| TaskManager | One container with two task slots |
| Jobs | Validation and feature jobs, each at parallelism 1 |
| Application runtime | Python 3.11 in the Flink image |
| Source interchange | Existing FeatureForge Parquet events |
| Streaming transport and output | Kafka topics |
| Materializer runtime | Host Python environment with the `replay` extra |
| Online store | Redis 7.4 with guarded writes and validated reads |
| Local persistence | Named Docker volumes for Kafka and Redis |

This is a local reference setup, not a highly available deployment.

### Jobs and Materializer

| Component | Entry point |
|---|---|
| Validation and error routing | `jobs/validate_and_route_events.py` |
| Rolling-hour play count | `jobs/user_play_count_1h.py` |
| Kafka-to-Redis materializer | `python -m featureforge_stream.materialize` |

Both Flink jobs support configurable topics, consumer groups, job names,
and bounded replay execution.

Without additional arguments, the Flink jobs retain their default topics
and continuous execution behavior.

The materializer requires an explicit consumer group. Its topic and Redis
key prefix are configurable.

### Default Kafka Topics

| Topic | Purpose |
|---|---|
| `interaction-events.v1` | Raw interaction-event JSON |
| `interaction-events.validated.v1` | Validated and normalized events |
| `interaction-events.rejected.v1` | Input-validation failures |
| `features.user-play-count-1h.v1` | Feature updates |
| `interaction-events.user-play-count-1h.late.v1` | Events too late for all assigned feature windows |

Historical replay uses dedicated topics and fresh job state. It does not send
old events into a test job whose watermark has already advanced.

## Interaction Event Contract

`InteractionEventV1` validates:

- UUID event identifiers.
- Required event fields.
- Non-empty synthetic user and content IDs without whitespace.
- Supported event types: `impression`, `play`, `skip`, and `complete`.
- Supported sources: `web`, `mobile`, and `tv`.
- Timezone-aware timestamps normalized to UTC.
- Ingestion timestamps on or after event timestamps.
- Strict non-negative integer playback durations.
- Zero playback duration for impression events.
- Schema version `v1`.
- Rejection of unknown fields.

The model supports JSON parsing and serialization and prevents normal field
reassignment after validation.

Invalid input is routed to the rejected topic with:

- An error schema version.
- A reason code.
- The original payload.
- Structured validation errors.

The validation integration test uses unique records for each run and checks
that a malformed record does not prevent a following valid record from
reaching the validated topic.

Contract:
[Interaction Events V1](docs/contracts/interaction-events-v1.md)

## FeatureForge Source Adapter

Entry point:

```text
src/featureforge_stream/replay.py
```

The adapter reads an existing FeatureForge `events.parquet` file and converts
supported deliveries into `InteractionEventV1` records.

### Supported Source Events

| Source event type | Current adapter behavior |
|---|---|
| `play` | Adapt and publish |
| `impression` | Adapt and publish |
| `watch` | Exclude and report |
| `click` | Exclude and report |
| `like` | Exclude and report |
| `search` | Exclude and report |

Unsupported source types are not silently reinterpreted. In particular,
`watch` is not renamed to `complete`.

This is a deliberately limited first integration, not complete support for
every FeatureForge source-event type.

### Field Mapping

| FeatureForge source | Streaming contract |
|---|---|
| `event_id` | Deterministic UUID derived from dataset identity and source event ID |
| `user_id` | Preserved |
| `content_id` | Preserved for supported events |
| `event_type` | Preserved for supported types |
| `event_time` | Preserved |
| `ingested_at` | Preserved |
| `watch_seconds` | Mapped to `play_seconds` for supported types |
| `device_type` | Mapped to `source` |
| `session_id`, `is_duplicate`, `is_late` | Not included in the current streaming payload |

Device mapping:

| Source device | Streaming source |
|---|---|
| `web` | `web` |
| `android` | `mobile` |
| `ios` | `mobile` |
| `mobile` | `mobile` |
| `tv` | `tv` |

Unknown device values on supported records fail adaptation.

The adapter does not change invalid impression durations to zero. The event
contract rejects incompatible values.

### Identity and Ordering

The dataset identifier participates in UUID generation:

- Repeating the same dataset with the same identifier preserves adapted IDs.
- Repeated source IDs remain repeated adapted IDs.
- Independent datasets must use distinct dataset identifiers.
- Conflicting adapted logical payloads sharing an ID fail validation.

Repeated deliveries are retained for downstream deduplication.

Deliveries are sorted by `ingested_at`. Equal ingestion timestamps retain
their original file order.

Historical event and ingestion timestamps are preserved. Replay is accelerated:
it does not wait for historical wall-clock intervals between deliveries.

### Preview and Publication

The default command is read-only with respect to Kafka:

```bash
python -m featureforge_stream.replay \
  --input ../featureforge/output/source_data/events.parquet \
  --dataset-id featureforge-source-v1
```

Publication requires both `--send` and an explicit replay topic:

```bash
python -m featureforge_stream.replay \
  --input ../featureforge/output/source_data/events.parquet \
  --dataset-id featureforge-source-v1 \
  --send \
  --topic "interaction-events.replay.${RUN_ID}.v1"
```

Set `RUN_ID` and create the topic before publication. The complete procedure
appears in the historical replay section below.

The topic must already exist and must use the
`interaction-events.replay.` prefix. The current producer sends to partition
zero, matching the single-partition local replay setup.

The adapter validates all supported records before publication. Delivery
callbacks and a final flush check broker delivery outcomes.

Producer idempotence does not make a separate manual rerun a no-op. Repeating
the send command can append the dataset again. After a partial publication
failure, inspect the topic before deciding how to retry.

The publication report is printed to the terminal; it is not currently
persisted as a replay manifest.

## Feature: User Play Count 1h

### Reference Definition

`count_user_plays_1h` counts unique play events for an explicitly requested
user within:

```text
(observation_time - 1 hour, observation_time]
```

The lower boundary is excluded; the upper boundary is included.

The reference:

- Counts repeated deliveries of the same logical event once.
- Rejects conflicting logical payloads sharing an event ID.
- Validates identities before applying query filters.
- Uses explicit, timezone-aware observation timestamps.
- Returns zero when no eligible event exists.
- Produces the same count regardless of input ordering.
- Preserves supplied event objects.

The calculation reconstructs event-time values from supplied records.
An event can contribute even when its `ingested_at` timestamp is later than
the observation timestamp.

Contract:
[User Play Count 1h V1](docs/contracts/user-play-count-1h-v1.md)

### Streaming Implementation

The feature job consumes validated events and computes one-hour windows
at minute-aligned observation times.

| Setting | Current value |
|---|---|
| Entity | `user_id` |
| Contribution | One per unique `play` event |
| Window duration | 1 hour |
| Observation-time interval | 1 minute |
| Watermark delay | 10 minutes |
| Allowed lateness | 30 minutes per Flink window |
| Deduplication key | `event_id` |
| Deduplication retention | 24 hours of processing time |
| Checkpoint interval | 10 seconds |
| Kafka sink delivery | `AT_LEAST_ONCE` |

These are explicit local policy choices, not universal production defaults.

### Window Boundaries

The feature contract uses `(start, end]`, while Flink's native time windows
use `[start, end)`.

`contracts/feature_time.py` maps the window-assignment timestamp so that
minute-aligned Flink windows preserve the reference's boundary membership.
The original event timestamps remain unchanged.

Unit tests compare this mapping with the reference at microsecond boundary
cases.

Streaming observation times are minute-aligned. The reference supports
arbitrary timezone-aware observation timestamps.

### Event Identity and Deduplication

Logical event identity is defined by `event_id`.

The logical payload includes:

- `event_time`.
- `user_id`.
- `content_id`.
- `event_type`.
- `play_seconds`.
- `source`.
- `schema_version`.

`ingested_at` is receipt metadata and is excluded from logical-payload
comparison.

Within retained deduplication state:

- Repeated delivery of the same logical event is suppressed.
- A conflicting logical payload raises `EventIdentityConflictError`.
- Repeated reads do not extend the retention period.

Deduplication happens before user-level window aggregation.

The 24-hour processing-time TTL bounds identity retention. It is not an
unlimited replay guarantee. After expiration, an event ID can be treated
as new again.

### Feature Output

Example record:

```json
{
  "feature_schema_version": "v1",
  "feature_name": "user_play_count_1h",
  "user_id": "user_000001",
  "observation_time": "2026-01-01T12:00:00Z",
  "window_start_exclusive": "2026-01-01T11:00:00Z",
  "play_count": 3
}
```

The feature topic is an update stream.

An accepted late event can produce another value for the same
`user_id` and `observation_time`. Records must not be interpreted as
exclusively final results or summed as independent contributions.

A single event belongs to multiple overlapping windows. Feature-output
record counts therefore do not equal input-event counts or user counts.

### Late Events and Event-Time Progress

Allowed lateness is evaluated using Flink's window lifecycle.

An event can still update some assigned sliding windows while other assigned
windows are already closed.

An event too late for all assigned windows is routed to the late topic with
its original payload and this reason code:

```text
TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS
```

The separate arrival-order admission reference is a semantic reference,
not a claim that its per-event classification is identical to Flink's
window-based lateness handling.

Event-time windows depend on watermark progress. Waiting in wall-clock time
does not by itself close the last windows of a quiet, unbounded Kafka stream.

The continuous-job integration tests supply explicit event-time progress.

A bounded replay has a defined source end. It does not require rewriting
historical timestamps into the future to finish the run.

Periodic watermark emission and operator execution affect the accelerated
replay's observed late-event behavior. This replay is not a deterministic
proof of every lateness boundary.

### Empty Windows

The current job does not generate periodic output for completely empty
windows or unknown users.

A window containing only non-play events can produce a zero play count.

The job does not yet emit automatic zero updates after all of a user's
events expire. The reference's ability to return zero for an explicit query
is therefore not a complete online-serving contract.

## Redis Online Store

Implementation:

```text
src/featureforge_stream/online_store.py
```

The Redis store is implemented independently of Kafka consumption and is
reused by the Kafka-to-Redis materializer.

It validates complete feature payloads and stores one value per user under
a configurable key prefix.

The default prefix is:

```text
featureforge-stream:online:v1:user_play_count_1h
```

### Current Write Policy

| Incoming feature | Result |
|---|---|
| No stored feature | Store and return `written` |
| Newer observation time | Replace and return `written` |
| Older observation time | Preserve current value and return `stale` |
| Same observation time, identical count | Return `unchanged` |
| Same observation time, higher count | Accept additive correction and return `written` |
| Same observation time, lower count | Preserve current value and return `stale` |

A newer window can replace a higher count with zero.

Same-window monotonicity is specific to the current append-only play count.
It is not a generic ordering policy for ratios, retractions, or arbitrary
feature corrections.

This policy cannot repair an already inflated count.

The store result `stale` describes the ordering of an incoming feature
update relative to the stored value. It does not mean the source event was
classified as too late by Flink.

### Concurrency and Reads

Comparison and writing use Redis optimistic locking with
`WATCH` / `MULTI` / `EXEC`.

Concurrent modifications trigger a reread and retry, bounded to 16 attempts.
Connection errors and exhausted retries propagate to the caller.

A missing key returns `None`, not zero. Invalid stored feature payloads fail
validation rather than being treated as missing.

The caller owns the Redis client's lifecycle.

### Verified Redis Behavior

Ten integration cases cover:

- Round trips for zero and nonzero counts.
- Missing-user lookup.
- Replacement by a newer window, including zero.
- Rejection of older windows.
- Identical repeated delivery.
- Increasing and decreasing same-window values.
- Preservation of a late correction against an older replayed value.
- A real concurrent modification causing `WatchError`, followed by a retry
  that preserves the competing newer value.

These store-level tests do not establish materializer crash recovery or
Redis/Feast storage compatibility.

## Kafka-to-Redis Materializer

Implementation:

```text
src/featureforge_stream/materialize.py
```

Entry point:

```bash
python -m featureforge_stream.materialize --help
```

The materializer consumes Kafka feature updates and applies the existing
Redis guarded-write policy.

### Current Behavior

The materializer:

- Supports a single-partition feature topic.
- Uses IPv4 for Kafka broker connections.
- Checks Redis connectivity before consumption.
- Validates each feature payload.
- Rejects feature tombstones.
- Disables automatic Kafka offset commits.
- Processes the Redis write result before committing the next Kafka offset.
- Commits periodically and after the final partial batch.
- Resumes from a committed group offset when available.
- Otherwise starts at the earliest retained offset.
- Clamps an older committed offset to the earliest retained offset.
- Captures the Kafka end offset at startup for bounded execution.
- Raises an error when the retained topic range is empty.
- Returns without writing when a bounded group is already caught up.
- Prints aggregate write-result counts at completion.
- Closes its Kafka consumer on completion or failure.

Offset clamping does not recover missing history. A committed Kafka offset
also does not prove that previously materialized Redis keys still exist.

Deleting Redis data while retaining the materializer's Kafka offsets requires
a deliberate rebuilding procedure.

### Arguments

| Argument | Behavior |
|---|---|
| `--bootstrap-servers` | Defaults to `127.0.0.1:9092` |
| `--redis-url` | Defaults to `redis://127.0.0.1:6379/0` |
| `--topic` | Defaults to `features.user-play-count-1h.v1` |
| `--group-id` | Required |
| `--key-prefix` | Defaults to `featureforge-stream:online:v1:user_play_count_1h` |
| `--commit-every` | Positive interval; defaults to 1,000 processed records |
| `--bounded` | Stops at the end offset captured at startup |

The current empty-topic check also applies without `--bounded`. An unbounded
invocation therefore requires a non-empty retained topic at startup.

### Delivery and Failure Boundaries

Redis writes and Kafka offset commits are not one atomic transaction.

If a process fails after a successful Redis write but before the corresponding
Kafka commit, the record may be processed again after restart. The store's
guarded-write policy is designed to handle repeated compatible updates, but
a controlled process-failure recovery test has not yet been demonstrated.

Malformed records and failed store operations propagate as errors. The failed
record is not deliberately committed as successfully processed.

A bounded run's stopping point is the captured startup end offset. Records
appended later are outside that run's intended range.

The current implementation has no periodic progress summary and no
application-level overall deadline for its polling loop. A missing terminal
summary is not by itself proof of either progress or failure.

### Unit-Test Coverage

Eleven tests in `tests/unit/test_materialize.py` cover:

- Redis processing before a synchronous Kafka commit.
- IPv4 client configuration and disabled automatic commits.
- No commit of a failed store operation.
- Bounded stopping at the captured end offset.
- Resume from a committed offset.
- Earliest-retained startup when no committed offset exists.
- Empty-topic failure and consumer cleanup.
- An already caught-up bounded consumer.
- Classification of store outcomes and the final partial commit.
- Invalid-payload failure without committing the record.
- Invalid commit-interval rejection before opening connections.
- The next-offset convention used by the commit helper.

Some tests verify multiple related assertions.

The tests use controlled doubles rather than live Kafka and Redis services.
They do not prove crash recovery, broker-failure recovery, or exactly-once
processing.

The module depends on `confluent-kafka`, supplied by the optional `replay`
extra. The materializer test file explicitly skips when that dependency is
not installed.

## Verified FeatureForge Replay — November 7, 2026

The completed local replay consumed:

```text
../featureforge/output/source_data/events.parquet
```

FeatureForge remained unchanged.

### Input and Adaptation

| Measurement | Observed value |
|---|---:|
| Source delivery records | 10,200 |
| Adapted deliveries | 3,424 |
| Adapted plays | 1,668 |
| Adapted impressions | 1,756 |
| Excluded deliveries | 6,776 |
| Unique adapted event IDs | 3,424 |
| Repeated IDs in the adapted selection | 0 |
| Validated Kafka records | 3,424 |
| Rejected Kafka records | 0 |

The source file's duplicate-generation behavior is not evidence that this
adapted selection contained duplicate IDs. It did not.

### Execution

Dedicated topics isolated the replay from the existing test flow:

| Topic | Purpose |
|---|---|
| `interaction-events.replay.ff-source-01.v1` | Adapted source deliveries |
| `interaction-events.replay.ff-source-01.validated.v1` | Validated deliveries |
| `interaction-events.replay.ff-source-01.rejected.v1` | Validation failures |
| `features.replay.ff-source-01.user-play-count-1h.v1` | Computed feature updates |
| `interaction-events.replay.ff-source-01.late.v1` | Too-late deliveries |

Both bounded jobs reached `FINISHED`.

The feature-computation job completed in 228,902 milliseconds, approximately
229 seconds. This is an observed local execution duration, not a throughput
benchmark or production performance claim.

### Source-Verified Examples

Five sampled output values were compared directly with unique play events
from the original Parquet source.

| User | Observation time, UTC | Flink count | Source count |
|---|---|---:|---:|
| `user_000456` | `2026-02-22T10:50:00Z` | 1 | 1 |
| `user_000182` | `2026-02-22T10:50:00Z` | 1 | 1 |
| `user_000346` | `2026-02-22T10:50:00Z` | 0 | 0 |
| `user_000346` | `2026-02-22T10:51:00Z` | 0 | 0 |
| `user_000182` | `2026-02-22T10:51:00Z` | 1 | 1 |

For example, `user_000456` had a source play at
`2026-02-22T10:17:40Z`, inside the reported window
`(2026-02-22T09:50:00Z, 2026-02-22T10:50:00Z]`.

All five examples matched.

These are source-level spot checks, not full batch-stream parity, proof of
complete output coverage, or a comparison with FeatureForge's existing
batch feature tables.

## Verified Redis Materialization — November 8, 2026

The retained raw and validated replay inputs each contained 3,424 records.

Before recomputation, the existing feature topic had no readable records:

```text
earliest_available_offset: 200399
end_offset: 200399
```

The existing feature topic was then configured with:

```text
message.timestamp.type=LogAppendTime
retention.ms=86400000
```

A fresh bounded feature-job submission read the retained validated input.
Job `fd64c0a9afdca431920fae95e7ebe47e` reached `FINISHED` in
225,792 milliseconds.

### Materializer Results

The materializer used:

```text
Topic:
features.replay.ff-source-01.user-play-count-1h.v1

Consumer group:
featureforge-stream-redis-ff-source-01

Redis key prefix:
featureforge-stream:replay:ff-source-01:user_play_count_1h
```

Observed results:

| Measurement | Observed value |
|---|---:|
| Starting offset | 200,399 |
| Captured end offset | 521,604 |
| Processed updates | 321,205 |
| `written` | 200,399 |
| `unchanged` | 390 |
| `stale` | 120,416 |
| Final committed offset | 521,604 |
| Final committed lag | 0 |

The three write-result counts sum to the processed update count:

```text
200399 + 390 + 120416 = 321205
521604 - 200399 = 321205
```

These are update-processing counts, not distinct user counts or a count of
unique feature windows.

The observed output volume is not proof of a particular number of logical
windows, sink duplicates, or corrections. That breakdown was not established.

A Kafka fetch timeout was logged during this invocation. The same invocation
subsequently completed and printed its result summary. The cause of the timeout
was not established.

### Redis/Reference Comparison

A read-only check loaded the retained validated Kafka input and compared
each stored user's play count with `count_user_plays_1h` at that user's stored
observation timestamp.

```json
{
  "source_records": 3424,
  "unique_event_ids": 3424,
  "source_users": 436,
  "redis_user_keys": 436,
  "checked_users": 436,
  "mismatches": 0,
  "mismatch_types": {},
  "examples": []
}
```

The check verified:

- One stored user key for every user in the validated input.
- No unexpected user keys under the replay prefix.
- Validated feature reads through `RedisUserPlayCountStore`.
- Matching reference counts at all 436 stored observation timestamps.

It did not independently verify:

- That every stored timestamp was the latest expected window.
- Every intermediate feature update.
- Full historical window coverage.
- The correctness of every late-event decision.
- Parity with FeatureForge's existing batch feature tables.
- Historical online availability.
- Present-time serving freshness.

The check was performed manually using an inline script. It is not yet a
version-controlled automated replay acceptance test.

### Retention Finding

Kafka logs showed deletion of historical segments from the replay late-event
topic.

An empty retained late-event topic therefore does not prove that the job
emitted zero late events.

The successful Redis comparison does not reconstruct or validate deleted
late-output history.

The historical replay procedure below now specifies timestamp and retention
settings before publication for future fresh replay topics. That documentation
change does not alter existing topics or restore deleted records.

## Golden Fixture and Streaming Integration Evidence

The version-controlled fixture is:

```text
fixtures/user_play_count_1h_v1.jsonl
```

It contains eight delivery records representing seven distinct logical
events, including a repeated event, window-boundary events, a non-play
event, and a second user.

At observation time `2026-01-01T12:00:00Z`:

| Requested user | Expected reference count |
|---|---:|
| `user_000001` | 3 |
| `user_000002` | 1 |
| `user_unknown` | 0 |

The golden integration case:

- Assigns unique IDs for each test run.
- Shifts fixture timestamps to a current minute-aligned observation time.
- Preserves relative event times and duplicate identity.
- Sends the fixture in event-time order.
- Advances event time beyond the observation window.
- Compares emitted counts with `count_user_plays_1h`.

Additional feature integration cases verify:

- Correction of a previously emitted window by a late event.
- Duplicate late-event delivery without count inflation.
- Routing of a valid event too late for all assigned windows.

The validation integration case verifies rejected-input isolation and
continued processing of valid input.

These cases do not establish restart recovery or end-to-end exactly-once
behavior.

## Local Development

Prerequisites:

- Python 3.11.
- Docker with Docker Compose.
- Sufficient Docker memory for Kafka, Flink, Redis, and Python workers.
- Available local ports `9092`, `8081`, and `6379`.

Run from the repository root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
make check
```

For source replay, Kafka-to-Redis materialization, and the materializer tests:

```bash
python -m pip install -e ".[dev,replay]"
```

PyFlink executes inside the custom Flink image. The base development
environment does not require a local PyFlink installation.

The `replay` extra provides PyArrow and the Kafka client used by the replay
producer and materializer.

Without the optional Kafka dependency, materializer unit tests explicitly
skip. The verified `131 passed, 14 deselected` run included that dependency.

## Run the Continuous Streaming Stack

### 1. Build and Start

```bash
docker compose build jobmanager

docker compose up -d \
  --wait \
  --wait-timeout 180 \
  kafka jobmanager taskmanager redis
```

Check Flink registration:

```bash
curl --fail --silent --show-error \
  http://localhost:8081/overview
```

On a fresh cluster before job submission, expect:

```json
{
  "taskmanagers": 1,
  "slots-total": 2,
  "slots-available": 2
}
```

The real response includes additional fields.

A started TaskManager container may not yet be registered. Confirm registration
before submitting jobs.

### 2. Create Default Topics

```bash
for topic in \
  interaction-events.v1 \
  interaction-events.validated.v1 \
  interaction-events.rejected.v1 \
  features.user-play-count-1h.v1 \
  interaction-events.user-play-count-1h.late.v1
do
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1
done
```

Historical replay must use the dedicated replay procedure below rather than
these default topics.

### 3. Submit Both Default Jobs

Check for existing jobs before submitting. Do not submit duplicate jobs
against the same default flow.

```bash
curl --fail --silent --show-error \
  http://localhost:8081/jobs/overview
```

Submit each job once:

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run \
  --detached \
  --python /opt/flink/jobs/validate_and_route_events.py
```

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run \
  --detached \
  --python /opt/flink/jobs/user_play_count_1h.py
```

Both jobs should report `RUNNING`.

Flink Web UI:

```text
http://localhost:8081
```

Use the Flink CLI inside the existing JobManager container. The documented
submission path is not `docker compose run jobmanager python ...`.

### 4. Run Targeted Integration Checks

Kafka/Flink cases require both default jobs running:

```bash
python -m pytest -q -m integration \
  tests/integration/test_validation_job.py \
  tests/integration/test_user_play_count_job.py
```

Redis cases require the Redis service:

```bash
python -m pytest -q -m integration \
  tests/integration/test_redis_online_store.py \
  tests/integration/test_redis_online_store_concurrency.py
```

With all prerequisites satisfied, the complete integration selection is:

```bash
python -m pytest -q -m integration tests/integration
```

The latest complete 14-case integration run has not yet been verified.
The previous targeted Redis run passed all ten cases.

The Kafka/Flink tests write synthetic records into the default topics and
advance event time. Historical replay must use a separate run with fresh
state rather than sending old source timestamps into those running jobs.

### 5. Inspect Default Feature Output

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic features.user-play-count-1h.v1 \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 10000
```

A timeout with zero returned records is not by itself a Flink-job failure.
Check job status, errors, and topic offsets separately.

### 6. Materialize Default Feature Output

After the default feature topic contains records:

```bash
python -m featureforge_stream.materialize \
  --topic features.user-play-count-1h.v1 \
  --group-id featureforge-stream-redis-online-v1 \
  --key-prefix featureforge-stream:online:v1:user_play_count_1h
```

Without `--bounded`, the materializer continues polling for updates.
The current implementation still rejects an empty topic at startup.

Use separate consumer groups and Redis prefixes for independent replay runs.

### 7. Stop the Local Stack

Stop host-side consumers deliberately before stopping their dependencies.

```bash
docker compose stop
```

This retains containers and named data volumes. It is not a Flink savepoint
or a demonstrated state-recovery procedure.

Inspect existing jobs after restarting the stack. Automatic restoration
across a complete local cluster restart has not been demonstrated.

Do not remove volumes unless deliberately resetting local data.

## Run a Historical FeatureForge Replay

The following procedure reuses the existing stack and job implementations.

It requires:

- The `replay` extra installed.
- An existing FeatureForge event Parquet file.
- A running Kafka, Flink, and Redis stack.
- At least one available Flink slot.
- Fresh replay topics.
- No competing producer writing to this replay's topics.

A default two-job setup uses both local slots. If necessary, deliberately
stop a selected development job before running a replay. Inspect job IDs
first; cancellation without a savepoint is not a recovery drill.

The jobs run sequentially: validation completes before feature computation
starts. This prevents the bounded feature source from taking an end-offset
snapshot of an intermediate topic that is still being populated.

The materializer runs after feature computation has successfully finished.

### 1. Choose a Fresh Run Identity

Run the following commands in the same shell:

```bash
RUN_ID="ff-source-$(date -u +%Y%m%dT%H%M%SZ)"

RAW_TOPIC="interaction-events.replay.${RUN_ID}.v1"
VALIDATED_TOPIC="interaction-events.replay.${RUN_ID}.validated.v1"
REJECTED_TOPIC="interaction-events.replay.${RUN_ID}.rejected.v1"
FEATURE_TOPIC="features.replay.${RUN_ID}.user-play-count-1h.v1"
LATE_TOPIC="interaction-events.replay.${RUN_ID}.late.v1"

REDIS_GROUP="featureforge-stream-redis-${RUN_ID}"
REDIS_PREFIX="featureforge-stream:replay:${RUN_ID}:user_play_count_1h"
```

Keep the dataset identity stable for the same source dataset. The run identity
separates Kafka topics, job execution, materializer offsets, and Redis keys;
it does not change source-event identity.

### 2. Create Fresh Topics with Explicit Retention Settings

```bash
for topic in \
  "$RAW_TOPIC" \
  "$VALIDATED_TOPIC" \
  "$REJECTED_TOPIC" \
  "$FEATURE_TOPIC" \
  "$LATE_TOPIC"
do
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 \
    --create \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 \
    --config message.timestamp.type=LogAppendTime \
    --config retention.ms=604800000 \
  || break
done
```

Stop if any creation fails. Do not silently reuse a partially populated run.

These settings are a local replay policy:

- Kafka record timestamps use broker append time.
- Time-based retention is configured to seven days.
- Historical JSON event and feature timestamps remain unchanged.
- The feature job assigns event time from the payload, not Kafka metadata.

Apply the policy before publication, including to the late-event output.
Historical Kafka metadata timestamps can otherwise make replay segments
eligible for deletion based on their apparent age.

Retention operates on Kafka log segments; it is not a precise per-record TTL.

The seven-day setting above is for future fresh runs. The verified November 8
feature topic used a 24-hour setting. This procedure does not change existing
topic configuration or restore deleted records.

Inspect a created topic's effective configuration if needed:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:29092 \
  --entity-type topics \
  --entity-name "$FEATURE_TOPIC" \
  --describe \
  --all
```

### 3. Preview and Publish

```bash
python -m featureforge_stream.replay \
  --input ../featureforge/output/source_data/events.parquet \
  --dataset-id featureforge-source-v1
```

After reviewing the report, publish once:

```bash
python -m featureforge_stream.replay \
  --input ../featureforge/output/source_data/events.parquet \
  --dataset-id featureforge-source-v1 \
  --send \
  --topic "$RAW_TOPIC"
```

Do not rerun publication blindly after an error or successful delivery.
Inspect the existing topic first.

On macOS, the replay producer may log failed IPv6 attempts when `localhost`
resolves to `::1` but Docker publishes the broker port only on IPv4.

The materializer explicitly restricts its Kafka connections to IPv4. That
does not change the separate replay producer's configuration.

### 4. Run Bounded Validation

The following submission is attached: wait for successful job completion
before proceeding.

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run \
  --python /opt/flink/jobs/validate_and_route_events.py \
  --input-topic "$RAW_TOPIC" \
  --validated-topic "$VALIDATED_TOPIC" \
  --rejected-topic "$REJECTED_TOPIC" \
  --group-id "featureforge-stream-validation-${RUN_ID}" \
  --job-name "featureforge-stream-validation-${RUN_ID}" \
  --bounded
```

Inspect output offsets:

```bash
for topic in "$VALIDATED_TOPIC" "$REJECTED_TOPIC"
do
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:29092 \
    --topic "$topic" \
    --time -1
done
```

For a fresh, single-partition, non-compacted topic starting at offset zero,
the end offset can be compared with delivered record counts. It is not a
universal record-count metric for arbitrary Kafka topics.

An end offset alone does not establish that earlier records remain readable.
Check earliest retained offsets when retention or deletion is in question.

### 5. Run Bounded Feature Computation

Only after validation has successfully finished:

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run \
  --python /opt/flink/jobs/user_play_count_1h.py \
  --input-topic "$VALIDATED_TOPIC" \
  --feature-topic "$FEATURE_TOPIC" \
  --late-topic "$LATE_TOPIC" \
  --group-id "featureforge-stream-user-play-count-${RUN_ID}" \
  --job-name "featureforge-stream-user-play-count-${RUN_ID}" \
  --bounded
```

`--bounded` reads from the earliest retained offsets to end offsets determined
for the source run. The jobs explicitly retain `STREAMING` runtime mode.

Wait for `FINISHED`. Submission or `RUNNING` status alone does not prove
successful completion.

A fresh feature-job submission explicitly uses
`KafkaOffsetsInitializer.earliest()`. A consumer-group offset reset is not
required to make that fresh submission read from the beginning of retained
input. Restored Flink state is a separate case.

### 6. Inspect Feature and Late Output

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic "$FEATURE_TOPIC" \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 10000
```

Inspect late-output offsets separately:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 \
  --topic "$LATE_TOPIC" \
  --time -1
```

Feature updates are not guaranteed to appear in global chronological order.

A large feature-output count is expected from overlapping windows and may
also include corrections or repeated sink deliveries. Do not interpret it
as a count of unique source events.

A reused topic's absolute end offset is not the output count of one new job.
New records are appended after the existing offset history.

### 7. Materialize the Completed Feature Output

```bash
python -m featureforge_stream.materialize \
  --topic "$FEATURE_TOPIC" \
  --group-id "$REDIS_GROUP" \
  --key-prefix "$REDIS_PREFIX" \
  --bounded
```

Wait for the final `store_results` JSON and successful process exit.

A result has this structure; categories with no observations can be omitted:

```json
{
  "store_results": {
    "stale": 120416,
    "unchanged": 390,
    "written": 200399
  }
}
```

The numbers above are the observed November 8 results, not expected counts
for every fresh run.

Do not sum feature counts across update records. The materializer applies
the store's window-ordering and same-window correction policy.

### 8. Read One Materialized User

Set a user ID present in this replay's input:

```bash
USER_ID="user_000456"
```

Read through the validated store API:

```bash
REDIS_PREFIX="$REDIS_PREFIX" USER_ID="$USER_ID" \
python -c '
import os
from redis import Redis
from featureforge_stream.online_store import RedisUserPlayCountStore

with Redis.from_url(
    "redis://127.0.0.1:6379/0",
    decode_responses=True,
    socket_connect_timeout=3,
    socket_timeout=3,
) as client:
    store = RedisUserPlayCountStore(
        client,
        key_prefix=os.environ["REDIS_PREFIX"],
    )
    feature = store.read(os.environ["USER_ID"])
    print("null" if feature is None else feature.model_dump_json(indent=2))
'
```

`null` means no key exists under the selected prefix for that user. It is not
a zero count.

A lookup demonstrates retrieval. It is not by itself a reference comparison
or a serving-freshness check.

## Operational Checks

### Inspect Flink Job Status

Replace `<JOB_ID>` with the ID returned by submission:

```bash
curl --fail --silent --show-error \
  "http://localhost:8081/jobs/<JOB_ID>"
```

For a compact status:

```bash
curl --fail --silent --show-error \
  "http://localhost:8081/jobs/<JOB_ID>" \
| python -c '
import json
import sys

job = json.load(sys.stdin)
print(json.dumps({
    "state": job["state"],
    "duration_ms": job["duration"],
}, indent=2))
'
```

Do not submit another replay merely because an attached terminal is quiet.

### Inspect Materializer Commits

Using the replay shell variables:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --group "$REDIS_GROUP" \
  --describe
```

Compare:

- `CURRENT-OFFSET`: confirmed processing position.
- `LOG-END-OFFSET`: current Kafka end.
- `LAG`: their offset distance.

The materializer directly assigns its partition. A report of no active group
members is not by itself evidence that this process is stopped.

Commit progress is periodic, not one update per processed record.

Lag zero does not independently establish Redis correctness. Check the
materializer completion output and validate stored values separately.

### Inspect Containers and Logs

```bash
docker compose ps
```

```bash
docker compose logs --no-color --since 15m --tail 100 kafka
```

```bash
docker compose logs --no-color --since 5m --tail 100 \
  jobmanager taskmanager
```

A container health check and a Flink job status answer different questions.
Neither alone proves correct external feature values.

## Reference Usage

Calculate the reference feature without Docker:

```python
from datetime import UTC, datetime
from pathlib import Path

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.reference.user_play_count import count_user_plays_1h

fixture_path = Path("fixtures/user_play_count_1h_v1.jsonl")

events = [
    InteractionEventV1.model_validate_json(line)
    for line in fixture_path.read_text(encoding="utf-8").splitlines()
    if line.strip()
]

count = count_user_plays_1h(
    events,
    user_id="user_000001",
    observation_time=datetime(2026, 1, 1, 12, tzinfo=UTC),
)

assert count == 3
```

The reference reconstructs event-time counts from all supplied records.
A streaming job may exclude events for windows already closed by its
watermark and lateness policy.

A reference comparison must therefore declare its input, observation times,
and treatment of late data rather than assume universal equivalence.

## Quality Checks

| Command | Purpose |
|---|---|
| `make check` | Run lint, formatting checks, and default tests |
| `make lint` | Run Ruff lint checks |
| `make format-check` | Check formatting without modifying files |
| `make format` | Format Python files |
| `make test` | Run default tests, excluding integration tests |
| `python -m pytest -q tests/unit/test_materialize.py` | Run materializer unit tests when the Kafka dependency is installed |
| `python -m pytest -q -m integration tests/integration` | Run integration cases when their infrastructure and jobs are available |

The `integration` marker is registered in `pyproject.toml`.
Default pytest options exclude it; an explicit `-m integration` selects it.

The default local test run does not execute PyFlink job imports inside the
Flink container. A green baseline is therefore not a substitute for submitting
the job in the actual runtime.

### Executable Evidence

| Concern | Evidence |
|---|---|
| Package metadata and import | `tests/unit/test_package.py` |
| Event structure, timestamps, and semantics | `tests/unit/test_events.py` |
| Valid/rejected payload routing | `tests/unit/test_event_routing.py` |
| Reference counts, duplicates, conflicts, and expiry | `tests/unit/test_user_play_count.py` |
| Arrival-order admission boundaries | `tests/unit/test_admission.py` |
| Reference-compatible window timestamp mapping | `tests/unit/test_feature_time.py` |
| Materializer write/commit ordering, bounded execution, resume, and errors | `tests/unit/test_materialize.py` |
| Kafka validation routing and continued valid processing | `tests/integration/test_validation_job.py` |
| Golden values, late corrections, duplicates, and too-late routing | `tests/integration/test_user_play_count_job.py` |
| Redis round trips and guarded update policy | `tests/integration/test_redis_online_store.py` |
| Real Redis WATCH conflict and retry | `tests/integration/test_redis_online_store_concurrency.py` |

The cross-project replay, original five source comparisons, and November 8
436-user Redis/reference check were performed manually. They are not yet
packaged as an automated full replay/parity acceptance test.

## Checkpointing and Delivery Guarantees

Both Flink jobs enable checkpointing at a 10-second interval.

Successful checkpoint creation has been observed locally. Inspect a running
job using its submitted job ID:

```bash
curl --fail --silent --show-error \
  "http://localhost:8081/jobs/<JOB_ID>/checkpoints"
```

Current guarantee boundaries:

- Kafka sinks use `AT_LEAST_ONCE`.
- Sources start from the earliest retained offsets on a fresh submission
  without restored state.
- A bounded run has explicit stopping offsets.
- Fresh submissions can reproduce historical output.
- Source-event deduplication does not imply deduplication of Kafka feature
  output across independent job submissions.
- The Redis materializer processes store results before committing offsets.
- Redis writes and Kafka commits are not atomic together.
- Checkpoint creation is not a demonstrated recovery drill.
- Durable, externally addressable checkpoint storage is not yet configured.
- End-to-end exactly-once delivery is not claimed.
- Replay safety beyond the deduplication retention period is not claimed.
- The Redis merge policy is limited to append-only play counts and does not
  repair incorrect upstream overcounts.
- Successful reads and reference counts do not establish online freshness.
- Retention configuration does not restore previously deleted records.

## Relevant Repository Structure

```text
featureforge-stream/
├── docker/
│   └── flink/
│       └── Dockerfile
├── docs/
│   └── contracts/
│       ├── interaction-events-v1.md
│       └── user-play-count-1h-v1.md
├── fixtures/
│   └── user_play_count_1h_v1.jsonl
├── jobs/
│   ├── validate_and_route_events.py
│   └── user_play_count_1h.py
├── src/
│   └── featureforge_stream/
│       ├── contracts/
│       │   ├── events.py
│       │   ├── feature_time.py
│       │   └── routing.py
│       ├── reference/
│       │   ├── admission.py
│       │   └── user_play_count.py
│       ├── materialize.py
│       ├── online_store.py
│       └── replay.py
├── tests/
│   ├── integration/
│   │   ├── test_redis_online_store.py
│   │   ├── test_redis_online_store_concurrency.py
│   │   ├── test_user_play_count_job.py
│   │   └── test_validation_job.py
│   └── unit/
│       ├── test_admission.py
│       ├── test_event_routing.py
│       ├── test_events.py
│       ├── test_feature_time.py
│       ├── test_materialize.py
│       ├── test_package.py
│       └── test_user_play_count.py
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── README.md
```

This is a focused view of the implemented path, not an exhaustive listing of
every repository file.

## Remaining Work

The first cross-project source-to-Redis path works. The complete platform
is not finished.

### Next Acceptance Milestone

Turn the manually verified replay-to-Redis path into a repeatable acceptance
workflow:

- Persist the reference-check script and machine-readable evidence.
- Verify latest expected observation-window coverage independently.
- Preserve and inspect late-event output under the declared retention policy.
- Exercise materializer interruption and restart.
- Rerun the complete integration selection with the current jobs and services.

This should reuse the current stream output, materializer, and store rather
than introduce a new general-purpose serving platform.

### Remaining Correctness and Platform Work

- Define and implement online freshness and expired-value behavior.
- Prove materializer retry behavior across process failure.
- Configure durable checkpoints and demonstrate controlled Flink recovery.
- Extend the documented feature set with appropriate update semantics.
- Write offline Parquet snapshots.
- Implement Spark/Flink and offline/online parity at declared cutoffs.
- Add a leakage-safe label contract.
- Define schema compatibility and migration behavior.
- Add operational metrics, dashboards, and concise runbooks.
- Package the constrained feature-specification and practitioner workflow.
- Persist replay evidence and automate the acceptance path.
- Complete architecture documentation, demo orchestration, and release
  packaging.

The implementation favors Flink and Kafka's built-in capabilities.
Additional abstractions or services are introduced only when required by
a concrete correctness, usability, or operational need.

## Scope and Non-Goals

This repository is a production-inspired local reference implementation,
not a production deployment or a claim of Netflix-scale operation.

It does not attempt to build:

- A second implementation of FeatureForge's batch platform.
- A custom streaming engine.
- A general-purpose feature compiler.
- A replacement for Flink, Kafka, or Feast.
- An unrelated recommendation or vector-search system.
- A Kubernetes platform.

AWS deployment considerations belong in an explicit, cost-aware production
mapping. Running the local stack does not establish AWS deployment,
high availability, or production readiness.

Project claims are limited to implemented behavior and observed or executable
evidence.