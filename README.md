# FeatureForge Stream

A production-inspired local reference implementation for near-real-time
feature computation.

FeatureForge Stream extends the batch-first foundation of
[FeatureForge](https://github.com/oster-dev/featureforge) with stateful,
event-time streaming and explicit contracts for event identity, time windows,
late arrivals, replay, and online/offline consistency.

The first connected source path is implemented:

```text
Existing FeatureForge Parquet events
    → contract adapter and Kafka producer
    → PyFlink validation
    → PyFlink user_play_count_1h computation
    → Kafka feature updates
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

The Redis store is implemented and tested separately. The completed replay's
Kafka feature output has not yet been connected to Redis.

### Latest Verified Results

```text
Ruff lint: passed
Ruff format: passed
Default test run: 120 passed, 14 deselected
Targeted Redis integration run: 10 passed
FeatureForge source adaptation: 3,424 records
Replay validation: FINISHED
Replay feature computation: FINISHED
Source-verified feature examples: 5/5 matched
```

The four streaming integration cases passed together earlier in the session,
before the subsequent job-configuration changes. The complete integration
suite was not rerun against the modified jobs during the final replay session.

The 14 integration cases comprise:

- Four Kafka/Flink integration cases.
- Ten Redis integration cases.

They are excluded from the default test run.

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
                  │
                  │ connection not yet implemented
                  ▼
          Redis online store
          separately implemented and tested
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
| Online-store component | Redis 7.4, separately integrated and tested |
| Local persistence | Named Docker volumes for Kafka and Redis |

This is a local reference setup, not a highly available deployment.

### Jobs

| Job | Entry point |
|---|---|
| Validation and error routing | `jobs/validate_and_route_events.py` |
| Rolling-hour play count | `jobs/user_play_count_1h.py` |

Both jobs support configurable topics, consumer groups, job names, and bounded
replay execution.

Without additional arguments, they retain their default topics and continuous
execution behavior.

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
  --topic interaction-events.replay.<RUN_ID>.v1
```

Replace `<RUN_ID>` before executing.

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

## Redis Online-Store Component

Implementation:

```text
src/featureforge_stream/online_store.py
```

The Redis store is implemented independently of Kafka consumption.

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

These tests do not establish Kafka-to-Redis materialization, crash recovery,
or Redis/Feast storage compatibility.

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

For the FeatureForge Parquet adapter:

```bash
python -m pip install -e ".[dev,replay]"
```

PyFlink executes inside the custom Flink image. The base development
environment does not require a local PyFlink installation.

The `replay` extra provides PyArrow and the Kafka producer client.

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

On a fresh cluster before job submission, expect one TaskManager and two
available slots.

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
The latest targeted Redis run passed all ten cases.

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

### 6. Stop the Local Stack

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
- At least one available Flink slot.
- Fresh replay topics.
- No competing producer writing to this replay's topics.

A default two-job setup uses both local slots. If necessary, deliberately
stop a selected development job before running a replay. Inspect job IDs
first; cancellation without a savepoint is not a recovery drill.

The jobs run sequentially: validation completes before feature computation
starts. This prevents the bounded feature source from taking an end-offset
snapshot of an intermediate topic that is still being populated.

### 1. Choose a Fresh Run Identity

Run the following commands in the same shell:

```bash
RUN_ID="ff-source-$(date -u +%Y%m%dT%H%M%SZ)"

RAW_TOPIC="interaction-events.replay.${RUN_ID}.v1"
VALIDATED_TOPIC="interaction-events.replay.${RUN_ID}.validated.v1"
REJECTED_TOPIC="interaction-events.replay.${RUN_ID}.rejected.v1"
FEATURE_TOPIC="features.replay.${RUN_ID}.user-play-count-1h.v1"
LATE_TOPIC="interaction-events.replay.${RUN_ID}.late.v1"
```

Keep the dataset identity stable for the same source dataset. The run identity
separates Kafka topics and job execution; it does not change source-event
identity.

### 2. Create Fresh Topics

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
  || break
done
```

Stop if any creation fails. Do not silently reuse a partially populated run.

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

On macOS, the local producer may log failed IPv6 connection attempts when
`localhost` resolves to `::1` but the published Docker port is IPv4-only.
The observed run subsequently completed delivery. Cleaning up this client
address configuration remains a small operational follow-up.

### 4. Run Bounded Validation

The following submission is attached: wait for successful job completion
before proceeding.

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run \
  -py /opt/flink/jobs/validate_and_route_events.py \
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

### 5. Run Bounded Feature Computation

Only after validation has successfully finished:

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run \
  -py /opt/flink/jobs/user_play_count_1h.py \
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

### 6. Inspect Results

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

## Quality Checks

| Command | Purpose |
|---|---|
| `make check` | Run lint, formatting checks, and default tests |
| `make lint` | Run Ruff lint checks |
| `make format-check` | Check formatting without modifying files |
| `make format` | Format Python files |
| `make test` | Run default tests, excluding integration tests |
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
| Kafka validation routing and continued valid processing | `tests/integration/test_validation_job.py` |
| Golden values, late corrections, duplicates, and too-late routing | `tests/integration/test_user_play_count_job.py` |
| Redis round trips and guarded update policy | `tests/integration/test_redis_online_store.py` |
| Real Redis WATCH conflict and retry | `tests/integration/test_redis_online_store_concurrency.py` |

The FeatureForge replay and five source comparisons were performed manually.
They are not yet packaged as an automated full replay/parity acceptance test.

## Checkpointing and Delivery Guarantees

Both jobs enable checkpointing at a 10-second interval.

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
- Checkpoint creation is not a demonstrated recovery drill.
- Durable, externally addressable checkpoint storage is not yet configured.
- End-to-end exactly-once delivery is not claimed.
- Replay safety beyond the deduplication retention period is not claimed.
- The Redis merge policy is limited to append-only play counts and does not
  repair incorrect upstream overcounts.

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

The first cross-project streaming path works. The complete platform is not
finished.

### Next Functional Milestone

Connect Kafka feature updates to the tested Redis store and demonstrate a
lookup of a value computed from FeatureForge source events.

This should reuse the current stream output and store rather than introduce
a new general-purpose serving platform.

### Remaining Correctness and Platform Work

- Rerun the complete integration selection with the updated jobs and all
  required services available.
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