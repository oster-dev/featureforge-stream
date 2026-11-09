# FeatureForge Stream

FeatureForge Stream is a local, production-inspired streaming feature platform
built with Kafka, Apache Flink, and Redis.

It validates interaction events, computes event-time features, handles late
and duplicate events explicitly, publishes feature updates to Kafka, and
materializes online values into Redis using feature-specific write policies.

Two features are currently implemented:

- `user_play_count_1h`
- `user_skip_rate_24h`

The project prioritizes explicit contracts, reproducible local evidence, and
honest guarantee boundaries over claims of production scale.

## Part of the FeatureForge system

```text
FeatureForge Batch
Source events, batch features, and historical retrieval
        |
        v
FeatureForge Stream
Event-time computation, online materialization,
and declared streaming snapshots
        |
        v
Future ML workflow / feature consumption
```

Project 1 remains independently owned and unchanged by streaming replay.
Downstream training integration is an explicit future boundary, not an
already deployed workflow.

## What this project demonstrates

- Strict event and feature contracts
- Kafka-based event transport
- Stateful Flink event-time computation
- Right-closed feature-window semantics
- Watermarks, allowed lateness, and feature-specific late outputs
- Logical duplicate suppression with finite state retention
- Deterministic Python feature references
- Guarded Kafka-to-Redis materialization
- Feature-specific same-window update policies
- Bounded historical replay
- Play-count Parquet snapshots and consistency reports
- Source-to-snapshot verification and late-output reconciliation
- A documented manual TaskManager recovery drill
- Unit tests and separately executed infrastructure integration tests

This is a local reference implementation. Each proof applies to its declared
feature, execution path, and scenario.

## Current implementation and evidence

| Capability | Current evidence | Important boundary |
|---|---|---|
| Event validation | Contract and Flink routing tests | Explicit V1 event contract |
| Play-count computation | Python reference and Flink integration tests | One-hour event-time feature |
| Skip-rate computation | Python reference and eight isolated Flink cases | 24-hour feature, boundaries, late corrections, duplicates, zero denominator, and late routing |
| Play-count materialization | Kafka/Redis verification and Redis tests | Single-partition append-only count semantics |
| Skip-rate Redis store | Policy tests and twelve Redis integration cases | Componentwise count ordering within a consistent run |
| Skip-rate materializer | Thirteen unit tests and a real Kafka-to-Redis integration test | Controlled feature payloads published directly to Kafka |
| Historical snapshots | Play-count Parquet export and Redis comparison | Bounded, run-scoped artifact |
| Source reconciliation | Source-derived count/coverage checks and verified late output | Declared play-count replay scope |
| Checkpoint recovery | Manual TaskManager failure drill | Specific play-count scenario |

The skip-rate Flink path and Kafka-to-Redis path have been tested separately.
A single combined interaction-events-to-Flink-to-Redis skip-rate execution is
not yet claimed.

## Features

### user_play_count_1h

Count unique play events for one user within:

```text
(observation_time - 1 hour, observation_time]
```

Definition:

```text
user_play_count_1h(user, observation_time)
= count of unique play events
  where event_time > observation_time - 1 hour
    and event_time <= observation_time
```

Only `play` events increment the count.

`impression`, `skip`, and `complete` are valid events but do not increment
this feature.

The Python reference returns zero for an empty eligible event set.

Contract:

[User Play Count 1h V1](docs/contracts/user-play-count-1h-v1.md)

### user_skip_rate_24h

For one user within:

```text
(observation_time - 24 hours, observation_time]
```

compute:

```text
skip_rate = skip_count / (play_count + skip_count)
```

Rules:

- Counts use unique logical events.
- A zero denominator returns `0.0`.
- Impression and complete events contribute to neither count.
- The rate is a float between `0.0` and `1.0`, not a percentage.
- No session-level pairing between plays and skips is inferred.
- Output includes both counts and the rate.

Examples:

| Plays | Skips | Skip rate |
|---:|---:|---:|
| 0 | 0 | 0.0 |
| 1 | 0 | 0.0 |
| 0 | 1 | 1.0 |
| 1 | 1 | 0.5 |
| 2 | 1 | 1/3 |
| 3 | 2 | 0.4 |

Contract:

[User Skip Rate 24h V1](docs/contracts/user-skip-rate-24h-v1.md)

The feature-contract document originally introduced the Python reference.
The streaming and online implementation status is recorded in the evidence
table above and in [ARCHITECTURE.md](ARCHITECTURE.md).

## Architecture

```text
FeatureForge Parquet source
            |
            v
Optional replay adapter
            |
            v
Raw interaction-event topic
            |
            v
+--------------------------------+
| Validation job                 |
| Kafka -> Flink -> Kafka        |
+--------------------------------+
       |                    |
       v                    v
Validated events       Rejected events
       |
       +-------------------------------+
       |                               |
       v                               v
+-------------------------+  +-------------------------+
| user_play_count_1h      |  | user_skip_rate_24h      |
| Kafka -> Flink -> Kafka |  | Kafka -> Flink -> Kafka |
+-------------------------+  +-------------------------+
       |           |              |            |
       v           v              v            v
Play-count     Play-count     Skip-rate     Skip-rate
updates        late output    updates       late output
       |                          |
       v                          v
Play-count materializer     Skip-rate materializer
       |                          |
       v                          v
Redis latest value          Redis latest value

Play-count output
       |
       v
Bounded Parquet snapshot + verification report
       |
       v
Snapshot/Redis comparison and source reconciliation
```

The diagram describes the implemented component relationships.
It does not imply that every end-to-end combination has already been tested.

See [ARCHITECTURE.md](ARCHITECTURE.md) for state lifecycle, time semantics,
write policies, replay boundaries, and remaining work.

## Kafka topics

| Default topic | Purpose |
|---|---|
| `interaction-events.v1` | Raw input events |
| `interaction-events.validated.v1` | Contract-valid events |
| `interaction-events.rejected.v1` | Invalid JSON or contract violations |
| `features.user-play-count-1h.v1` | Play-count updates |
| `interaction-events.user-play-count-1h.late.v1` | Play-count events too late for all assigned windows |
| `features.user-skip-rate-24h.v1` | Skip-rate updates |
| `interaction-events.user-skip-rate-24h.late.v1` | Skip-rate events too late for all assigned windows |
| `interaction-events.replay.<run-name>.v1` | Dedicated historical replay input |

Feature jobs accept input, feature-output, late-output, group, and job-name
overrides for isolated runs.

Kafka auto-topic creation is disabled.

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

### Validation rules

| Field | Rule |
|---|---|
| `event_id` | UUID |
| `event_time` | Timezone-aware timestamp |
| `ingested_at` | Timezone-aware timestamp |
| `user_id` | Non-empty string without whitespace |
| `content_id` | Non-empty string without whitespace |
| `event_type` | `impression`, `play`, `skip`, or `complete` |
| `play_seconds` | Strict non-negative integer |
| `source` | `web`, `mobile`, or `tv` |
| `schema_version` | `v1` |

Additional rules:

- Timestamp strings must contain an ISO-8601 datetime with a `T` separator.
- Timestamps are normalized to UTC.
- `ingested_at >= event_time`.
- Impression events have `play_seconds = 0`.
- Unknown fields are rejected.

Detailed contract:

[Interaction Events V1](docs/contracts/interaction-events-v1.md)

### Rejected records

The validation job publishes rejected input to:

```text
interaction-events.rejected.v1
```

Example:

```json
{
  "error_schema_version": "v1",
  "reason_code": "INVALID_JSON",
  "raw_payload": "{\"broken\":",
  "errors": []
}
```

| Reason code | Meaning |
|---|---|
| `INVALID_JSON` | Payload is not valid JSON |
| `EVENT_CONTRACT_VIOLATION` | JSON violates the event contract |

Validation failures are isolated per record.

Feature jobs normally consume validated input. Their own event parsing does
not replace the validation job's structured rejection path.

## Event-time behavior

Both features use event time, not Kafka arrival time or receipt time, for
window membership.

| Feature | Window length | Slide |
|---|---|---|
| `user_play_count_1h` | 1 hour | 1 minute |
| `user_skip_rate_24h` | 24 hours | 1 minute |

Both use right-closed windows:

```text
(observation_time - window_length, observation_time]
```

An event exactly at the lower boundary is excluded.
An event exactly at the upper boundary is included.

Flink's native half-open windows use a timestamp mapping that preserves these
boundaries at whole-millisecond window endpoints.

### Output cadence

Observation windows are minute-aligned, but outputs depend on watermark
progress. Wall-clock time alone does not cause a new result.

Accepted non-contributing events can create a window with a zero result.
The jobs do not generate windows for users with no input events or provide
an indefinite heartbeat after input stops.

### Watermarks and allowed lateness

Current configuration:

```text
Bounded out-of-orderness: 10 minutes
Allowed lateness:        30 minutes
```

Admission is determined per assigned window.

With sliding windows, an event can be too late for one assigned window while
still being accepted by another. Only an event too late for all assigned
windows is sent to the late output.

A simple age threshold is not an exact description of Flink's live
sliding-window admission.

### Late corrections

Play-count corrections can increase the count.

Skip-rate corrections can increase or decrease the rate:

```text
1 play + 1 skip  -> 0.5
2 plays + 1 skip -> 1/3
2 plays + 2 skips -> 0.5
```

A decreasing rate is not necessarily stale.

### Too-late output

Example:

```json
{
  "error_schema_version": "v1",
  "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
  "feature_name": "user_skip_rate_24h",
  "raw_payload": "<original event payload>"
}
```

Each feature has its own default late topic.

## Duplicate handling

Logical identity is `event_id`.

Repeated deliveries with the same logical payload are suppressed while
deduplication state is retained.

`ingested_at` is excluded from logical identity because it represents receipt
metadata, not the interaction's business meaning.

Conflicting logical payloads for a retained ID raise
`EventIdentityConflictError`.

### Retention boundary

| Feature job | Deduplication TTL |
|---|---|
| Play count | 24 hours |
| Skip rate | 48 hours |

TTL is measured in processing time from the first accepted delivery.
Repeated identical deliveries do not extend it.

After expiry, an event ID may be accepted again. This is finite-horizon
deduplication, not unlimited replay protection.

The Python references validate identities over the full supplied input.
Streaming conflict detection is bounded by retained state.

## Feature output examples

### Play count

Topic:

```text
features.user-play-count-1h.v1
```

```json
{
  "feature_schema_version": "v1",
  "feature_name": "user_play_count_1h",
  "user_id": "user_000001",
  "observation_time": "2026-01-02T12:00:00Z",
  "window_start_exclusive": "2026-01-02T11:00:00Z",
  "play_count": 3
}
```

### Skip rate

Topic:

```text
features.user-skip-rate-24h.v1
```

```json
{
  "feature_schema_version": "v1",
  "feature_name": "user_skip_rate_24h",
  "user_id": "user_000001",
  "observation_time": "2026-01-02T12:00:00Z",
  "window_start_exclusive": "2026-01-01T12:00:00Z",
  "play_count": 3,
  "skip_count": 2,
  "skip_rate": 0.4
}
```

The skip-rate model validates count types, window duration, finite rate,
rate/count consistency, entity syntax, and schema identity.

Both feature jobs use at-least-once Kafka sinks.
Repeated output records are therefore possible.

## Redis online stores

Redis holds the latest accepted observation window per user in the selected
feature namespace.

It is not a historical window store.

### Play-count write policy

Default key format:

```text
featureforge-stream:online:v1:user_play_count_1h:<user_id>
```

| Incoming feature | Decision |
|---|---|
| Newer observation time | `written` |
| Older observation time | `stale` |
| Same time, larger count | `written` |
| Same time, equal count | `unchanged` |
| Same time, smaller count | `stale` |

This supports append-only count corrections.
It does not support retractions or repair an inflated count.

### Skip-rate write policy

The skip-rate materializer requires an explicit Redis key prefix.

Example namespace:

```text
featureforge-stream:replay:my-run:user_skip_rate_24h
```

Within one observation window, compare:

```text
(play_count, skip_count)
```

| Incoming feature | Decision |
|---|---|
| Newer observation time | `written` |
| Older observation time | `stale` |
| Same time, identical counts | `unchanged` |
| Both counts at least as large, one larger | `written` |
| Both counts at most as large, one smaller | `stale` |
| One count increases and the other decreases | Raise `SkipRateUpdateConflictError` |

The store does not select the largest rate or invent a merged count vector.

This policy assumes a consistent append-only computation.
Independent runs require independent namespaces.

### Concurrency control

Both stores use `WATCH`, `MULTI`, and `EXEC`.

If the key changes before a transaction commits, the store retries with a
fresh read and reapplies the policy. Retries are limited to 16 attempts.

Invalid stored payloads, policy conflicts, connection errors, and exhausted
retries propagate to the caller.

## Local setup

### Requirements

- Python 3.11+
- Docker and Docker Compose
- Make
- A local Python virtual environment

The demonstrated Flink image uses Python 3.11.

### Create the environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,replay]"
```

The `replay` extra supplies Kafka and Parquet dependencies used by replay,
materializers, and related checks.

### Run local quality checks

```bash
make check
```

This runs lint, formatting checks, and the default test selection.

Integration tests are excluded from that default selection.

### Start the local stack

```bash
docker compose up -d --build
docker compose ps
```

Services:

```text
kafka
jobmanager
taskmanager
redis
```

Endpoints:

| Service | Endpoint |
|---|---|
| Kafka | `127.0.0.1:9092` |
| Flink UI / REST | `127.0.0.1:8081` |
| Redis | `127.0.0.1:6379` |

### Stop the stack

```bash
docker compose down
```

This removes containers without explicitly deleting named data volumes.

The following command deletes persisted Kafka and Redis data and should only
be used for an intentional local reset:

```bash
docker compose down -v
```

### Local security boundary

The stack is not hardened for production.

Kafka uses plaintext listeners. The configuration does not establish
production authentication, authorization, encryption, or secret management.

## Running tests

### Standard tests

```bash
make test
```

Or:

```bash
.venv/bin/python -m pytest -q
```

### Targeted feature references

```bash
.venv/bin/python -m pytest \
  tests/unit/test_user_play_count.py \
  tests/unit/test_user_skip_rate.py \
  tests/unit/test_skip_rate_update_policy.py \
  -q
```

### Isolated skip-rate Flink tests

Start the Compose stack first and ensure a Flink task slot is available.

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_user_skip_rate_job.py \
  -v -s -x
```

The eight cases cover:

- Late corrections and duplicate safety
- Five lower/upper boundary cases at microsecond precision
- Zero denominator with non-contributing events
- Too-late routing

Each case creates dedicated topics, submits its own feature job, and requests
cancellation of the identified job during cleanup.

Kafka topics remain available for diagnosis.

### Skip-rate Redis tests

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_redis_skip_rate_store.py \
  -v -x
```

These tests use isolated Redis prefixes and clean up their test keys.

They include a real concurrent-write conflict and retry.

### Skip-rate Kafka-to-Redis test

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_skip_rate_materialization.py \
  -v -s -x
```

This test publishes controlled feature payloads directly to Kafka and verifies:

- Stored values after each materialization phase
- Increasing and decreasing rates
- Stale and unchanged outcomes
- Newer zero-valued windows
- Committed offsets
- Resume and caught-up behavior
- Replay using a fresh consumer group

It does not start Flink.

### Other integration tests

Other integration tests cover validation routing, play-count computation, and
the original Redis store.

Some depend on existing jobs and topic setup. Inspect their prerequisites
before running the entire integration directory:

```bash
.venv/bin/python -m pytest -m integration
```

This command is not presented as a self-provisioning clean-clone demo.

A successful GitHub check does not by itself prove that every separately
executed integration test ran in CI.

## Running Flink jobs

Job scripts live under `jobs/` and are mounted into Flink containers at:

```text
/opt/flink/jobs
```

They are submitted through the Flink CLI, not through nonexistent
`featureforge_stream.jobs.*` Python modules.

### Create the default topics

For a fresh local stack, create the default topics explicitly:

```bash
for topic in \
  interaction-events.v1 \
  interaction-events.validated.v1 \
  interaction-events.rejected.v1 \
  features.user-play-count-1h.v1 \
  interaction-events.user-play-count-1h.late.v1 \
  features.user-skip-rate-24h.v1 \
  interaction-events.user-skip-rate-24h.late.v1
do
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 || break
done
```

This does not apply the historical replay-topic policy described below.

### Start validation

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run --detached \
  --python /opt/flink/jobs/validate_and_route_events.py
```

The default validation path reads the raw topic and writes validated and
rejected topics.

### Start the play-count job

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run --detached \
  --python /opt/flink/jobs/user_play_count_1h.py
```

### Start the skip-rate job instead

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink run --detached \
  --python /opt/flink/jobs/user_skip_rate_24h.py
```

The local TaskManager has two slots. Do not assume validation and both feature
jobs can all run concurrently with the current configuration.

Run validation plus one feature job, or use staged bounded execution.

Keep the submitted job ID. To stop that specific job:

```bash
docker compose exec -T jobmanager \
  /opt/flink/bin/flink cancel "<job-id>"
```

Replace the placeholder with the actual submitted ID.

### Code distribution

Mounted job-script changes are visible without rebuilding the image.

Changes to package code under `src/` require rebuilding/redeploying the image
when that code must be available inside Flink containers.

The demonstrated materializers run in the host virtual environment.

## Materializing features into Redis

Create and populate the selected feature topic before starting a materializer.

### Play count

```bash
.venv/bin/python -m featureforge_stream.materialize \
  --bootstrap-servers 127.0.0.1:9092 \
  --redis-url redis://127.0.0.1:6379/0 \
  --group-id featureforge-stream-redis-materializer-v1
```

For a completed isolated replay:

```bash
.venv/bin/python -m featureforge_stream.materialize \
  --bootstrap-servers 127.0.0.1:9092 \
  --redis-url redis://127.0.0.1:6379/0 \
  --topic features.replay.my-run.user-play-count-1h.v1 \
  --group-id materialize-my-run-play-count-v1 \
  --key-prefix featureforge-stream:replay:my-run:user_play_count_1h \
  --bounded
```

### Skip rate

```bash
.venv/bin/python -m featureforge_stream.materialize_skip_rate \
  --bootstrap-servers 127.0.0.1:9092 \
  --redis-url redis://127.0.0.1:6379/0 \
  --topic features.user-skip-rate-24h.v1 \
  --group-id materialize-my-run-skip-rate-v1 \
  --key-prefix featureforge-stream:replay:my-run:user_skip_rate_24h \
  --bounded
```

`my-run` is an example identifier. Use a unique group and namespace for an
independent run, and select the actual feature topic from that run.

The default shared feature topic is not automatically an isolated replay.
For isolated verification, override the job output topic and use that same
topic in the materializer.

### Resume versus independent replay

To resume interrupted materialization, retain its original:

- Feature topic
- Consumer group
- Redis key prefix

For an independent computation, use isolated topics and a new namespace.

Replaying the same retained output into an existing namespace is different
from mixing independently computed outputs.

### Offset and failure semantics

Materializers currently require exactly one Kafka partition.

In bounded mode they capture the exclusive end offset at startup and resume
from the group's committed offset or retained low offset.

They reject an empty readable range.
An already caught-up group returns without additional writes.

Offsets are committed after successful Redis processing, including guarded
`stale` and `unchanged` outcomes.

Redis writes and Kafka commits are not one transaction. A failure between
them can cause redelivery.

There is no overall or idle-progress timeout. Bounded mode defines an offset
boundary, not guaranteed wall-clock completion.

## Online lookup

After materialization, read the feature through its store.

Play count:

```python
from redis import Redis
from featureforge_stream.online_store import RedisUserPlayCountStore

with Redis.from_url(
    "redis://127.0.0.1:6379/0",
    decode_responses=True,
    socket_connect_timeout=3,
    socket_timeout=3,
) as client:
    store = RedisUserPlayCountStore(client)
    print(store.read("user_000001"))
```

Skip rate:

```python
from redis import Redis
from featureforge_stream.skip_rate_store import RedisUserSkipRateStore

with Redis.from_url(
    "redis://127.0.0.1:6379/0",
    decode_responses=True,
    socket_connect_timeout=3,
    socket_timeout=3,
) as client:
    store = RedisUserSkipRateStore(
        client,
        key_prefix="featureforge-stream:replay:my-run:user_skip_rate_24h",
    )
    print(store.read("user_000001"))
```

Use the exact namespace selected during materialization.
A missing user returns `None`.

## Historical replay

The Project 1 adapter currently supports source play and impression events.
The wider stream contract also supports skip and complete events.

Preview an adapted dataset without publishing:

```bash
.venv/bin/python -m featureforge_stream.replay \
  --input path/to/events.parquet \
  --dataset-id my-dataset
```

Publish to a dedicated replay topic:

```bash
.venv/bin/python -m featureforge_stream.replay \
  --input path/to/events.parquet \
  --dataset-id my-dataset \
  --send \
  --topic interaction-events.replay.my-run.v1 \
  --bootstrap-servers 127.0.0.1:9092
```

The adapter refuses publication into the normal live input topic.

### Historical replay topic policy

Create dedicated input, validated, rejected, feature, and late topics before
publishing.

For the declared local historical replay workflow, use:

```text
cleanup.policy=delete
retention.ms=604800000
message.timestamp.type=LogAppendTime
```

Example:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create \
  --topic features.replay.my-run.user-play-count-1h.v1 \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config message.timestamp.type=LogAppendTime
```

`LogAppendTime` affects Kafka transport timestamps, not JSON event, ingestion,
or observation timestamps.

Feature jobs assign event time from the JSON payload.

Historical output with `CreateTime` and time-based retention was previously
deleted during a local replay; broker logs confirmed the cause.

Inspect earliest and latest offsets together.
A large end offset does not prove records remain readable.
Changing retention cannot restore deleted records.

### Stage ordering

Run bounded stages in order:

1. Publish the complete replay input.
2. Run validation to `FINISHED`.
3. Verify validated and rejected output.
4. Run the selected feature job to `FINISHED`.
5. Verify feature and late output.
6. Materialize into an isolated Redis namespace.
7. Run the applicable verification.

Each bounded stage captures its own input boundary during startup.
Starting downstream too early can capture an incomplete range.

Accelerated replay retains source timestamps. Watermark scheduling can still
affect late admission; replay order alone does not prove identical admission
across executions.

See [ARCHITECTURE.md](ARCHITECTURE.md) for details.

## Verify play-count Kafka output against Redis

The following checker applies to the play-count feature, not skip rate.

After replay and materialization have finished:

```bash
.venv/bin/python scripts/verify_kafka_redis.py \
  --topic "<replay-feature-topic>" \
  --key-prefix "<replay-redis-key-prefix>" \
  --bootstrap-servers 127.0.0.1:9092 \
  --redis-url redis://127.0.0.1:6379/0 \
  --timeout-seconds 180
```

Replace placeholders with the topic and namespace from the same run.
Stop writers before verification.

The checker selects the latest observation window per user and the largest
play count for that window. It compares complete feature values and reports
missing, differing, and additional Redis values.

Prerequisites include:

- Exactly one Kafka partition
- A non-empty retained range beginning at offset zero
- Contiguous offsets and no tombstones
- Stable Kafka low/high offsets during reading
- Completed materialization and no concurrent writers

It does not write feature values or commit Kafka offsets.
Redis namespace scanning is not an atomic snapshot.

Successful output includes:

```text
PASS: latest Kafka feature values match Redis
```

This proves checked materialization consistency, not source completeness,
Spark/Flink parity, or recovery.

## Export a play-count replay snapshot

After replay and materialization have finished:

```bash
.venv/bin/python scripts/export_replay_snapshot.py \
  --topic "<replay-feature-topic>" \
  --key-prefix "<replay-redis-key-prefix>" \
  --output-dir "output/stream_snapshots/<unique-export-id>" \
  --timeout-seconds 180
```

Use matching topic/namespace values, stop writers, and choose a new output
directory.

The exporter creates:

```text
windows.parquet
verification.json
```

The report includes offset boundaries, row counts, the Parquet SHA-256, and
comparison results.

Successful output includes:

```text
PASS: persisted snapshot latest values match Redis
```

This is a run-scoped streaming artifact, not Project 1's canonical offline
store.

The exporter consolidates repeated play-count windows using maximum count.
That rule must not be reused as maximum skip rate.

Skip-rate snapshot support is not claimed.

The snapshot and report are not published as one crash-atomic unit.
An incomplete directory is not an accepted export.

See [Stream Snapshot V1](docs/contracts/user-play-count-1h-snapshot-v1.md).

## Compare source events with a persisted snapshot

Check play counts and complete minute-aligned window coverage against
independent Python event-time reconstruction:

```bash
.venv/bin/python scripts/verify_source_snapshot.py \
  --input-topic "<validated-replay-topic>" \
  --snapshot-dir "<existing-snapshot-directory>" \
  --report "<new-report-path>.json"
```

A full-input mismatch remains recorded even when observed late exclusions
can explain it.

Inspect the separate reconciliation:

```bash
.venv/bin/python scripts/reconcile_source_snapshot.py \
  --input-topic "<validated-replay-topic>" \
  --late-topic "<matching-feature-run-late-topic>" \
  --snapshot-dir "<existing-snapshot-directory>" \
  --report "<new-reconciliation-report-path>.json"
```

Use completed outputs from the same run and new report paths.
Keep local evidence under `output/stream_snapshots/`, outside Git.

### Recorded local result

The observed play-count replay contained:

- 3,424 unique input events
- 200,399 existing snapshot rows whose counts matched reconstruction
- 240 additional windows expected by full-input reconstruction
- Four payload-verified late events explaining the difference

Reconstruction from the remaining 3,420 events matched both counts and
complete window coverage.

`RECONCILED` means consistency with observed exclusions.
It is not independent proof of watermark decisions, Spark batch parity,
historical training safety, or recovery.

See [Source-to-Snapshot Verification V1](docs/contracts/source-snapshot-verification-v1.md).

## TaskManager checkpoint-recovery drill

A controlled local play-count drill killed and restarted the TaskManager
while keeping the JobManager and Kafka running.

The same job restored from checkpoint `23`.

Repeating an original play and adding a new play produced the reference
count `2`, with no excessive count observed during the next 15 seconds.

This is a documented manual drill, not:

- An automated recovery suite
- A skip-rate recovery proof
- A JobManager/full-cluster disaster-recovery proof
- An end-to-end exactly-once guarantee

See [TaskManager Checkpoint Recovery](docs/runbooks/taskmanager-checkpoint-recovery.md).

## Project structure

Selected implementation paths:

```text
featureforge-stream/
├── jobs/
│   ├── validate_and_route_events.py
│   ├── user_play_count_1h.py
│   └── user_skip_rate_24h.py
├── src/featureforge_stream/
│   ├── contracts/
│   │   ├── events.py
│   │   ├── feature_time.py
│   │   ├── routing.py
│   │   └── user_skip_rate.py
│   ├── reference/
│   │   ├── admission.py
│   │   ├── reconciliation.py
│   │   ├── snapshot_parity.py
│   │   ├── user_play_count.py
│   │   └── user_skip_rate.py
│   ├── materialize.py
│   ├── materialize_skip_rate.py
│   ├── online_store.py
│   ├── skip_rate_store.py
│   ├── replay.py
│   └── snapshot.py
├── scripts/
│   ├── create_replay_topics.py
│   ├── export_replay_snapshot.py
│   ├── verify_kafka_redis.py
│   ├── verify_source_snapshot.py
│   └── reconcile_source_snapshot.py
├── tests/
│   ├── unit/
│   └── integration/
├── fixtures/
├── docs/
│   ├── adr/
│   ├── contracts/
│   └── runbooks/
├── docker/flink/Dockerfile
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── README.md
└── ARCHITECTURE.md
```

## Technology choices

| Technology | Role |
|---|---|
| Python | Contracts, references, adapters, and materialization |
| Pydantic | Event and feature payload validation |
| Kafka | Retained event transport and stage boundaries |
| Apache Flink / PyFlink | Stateful event-time computation |
| Redis | Latest online feature values |
| Parquet / PyArrow | Bounded historical stream snapshots |
| Docker Compose | Local runtime |
| pytest | Unit and integration tests |
| Ruff | Linting and formatting |
| GitHub Actions | Automated repository checks |

Prometheus/Grafana, a deployed schema registry, Spark parity, and an AWS
production deployment are not listed as implemented runtime components.

## Design boundaries

This project does not claim:

- End-to-end exactly-once processing
- Unlimited replay or duplicate protection
- General feature retractions
- Repair of inflated counts
- Equivalence between independent computations sharing one namespace
- Multi-partition Redis materialization
- Complete Spark/Flink batch-stream parity
- Historical online availability or leakage-safe training retrieval
- Automatic Project 1 offline-store compatibility
- Skip-rate snapshots or skip-rate recovery
- A combined skip-rate events-to-Flink-to-Redis proof
- Recovery from every failure mode
- Production-scale performance, security, or availability
- A deployed downstream ML workflow

The 24-hour skip-rate window uses a one-minute slide, assigning an event to
1,440 windows. This is a deliberate local correctness configuration, not a
production performance claim.

## Remaining work

- Combined skip-rate events-to-Flink-to-Redis verification
- Skip-rate snapshot contract and consolidation policy
- Remaining planned features
- Leakage-safe label contract and explicit training handoff
- Declared batch-stream parity
- Practitioner specs, CLI, generated catalog, and notebook
- Operational metrics, dashboards, and additional runbooks
- Additional recovery evidence where useful
- Clean-clone demo and release packaging
- Documented local-to-AWS production mapping

## Roadmap alignment

FeatureForge Stream is the streaming-infrastructure layer of a connected
Data & Feature Infrastructure / ML Platform portfolio.

The engineering focus is:

- Precise feature and event contracts
- Event-time correctness
- Explicit state and retention boundaries
- Online-serving consistency
- Replay and recovery evidence
- Testable operational behavior
- Clear ownership across batch, streaming, and future ML workflows

The guiding implementation rule is:

> Define the contract, define the reference, test the semantics,
> then connect and verify the infrastructure.

## License

This project is licensed under the [MIT License](LICENSE).