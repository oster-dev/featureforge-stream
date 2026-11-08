# Architecture — FeatureForge Stream

FeatureForge Stream is a local, production-oriented streaming feature platform.
It consumes validated interaction events from Kafka, computes an event-time
feature with Apache Flink, publishes feature updates back to Kafka, and
materializes the latest online feature value into Redis.

The system is intentionally small, explicit, and testable. It demonstrates the
core responsibilities of a feature platform: contracts, event-time semantics,
stream processing, late data, deduplication, online materialization, and
verifiable correctness.

## System overview

```text
FeatureForge Parquet source
            |
            v
   Replay adapter (optional)
            |
            v
interaction-events.v1
            |
            v
+---------------------------+
| Validation job            |
| Kafka -> Flink -> Kafka   |
+---------------------------+
      |                  |
      v                  v
validated events    rejected events
      |
      v
+---------------------------+
| Feature job               |
| Kafka -> Flink -> Kafka   |
+---------------------------+
      |                  |
      v                  v
feature updates       too-late events
      |
      v
+---------------------------+
| Redis materializer        |
| Kafka -> Redis            |
+---------------------------+
      |
      v
Online feature lookup
```

## Core design principles

- **Event time is authoritative.** Feature windows are defined by `event_time`, not by Kafka arrival time or ingestion time.
- **Contracts come first.** Invalid input is rejected explicitly rather than silently coerced or dropped.
- **Correctness is observable.** A deterministic Python reference defines the expected feature semantics and is compared against the streaming implementation.
- **Late data is handled explicitly.** Events may correct an already emitted window, but events beyond the retention boundary are routed to a dedicated late topic.
- **Duplicates are suppressed by logical identity.** Repeated Kafka deliveries do not inflate a feature value.
- **Online writes are guarded.** Redis materialization prevents stale or regressive updates from overwriting newer feature state.
- **The system remains deliberately simple.** Kafka, Flink, and Redis are used directly; no additional orchestration or feature-store framework is introduced.

## Runtime components

| Component | Responsibility |
|---|---|
| Kafka | Durable event transport and decoupling between producers, processing, and materialization |
| Validation job | Validates raw interaction events and routes valid and rejected records to separate topics |
| Feature job | Deduplicates events, assigns event time, computes sliding one-hour play counts, and emits feature updates |
| Redis materializer | Consumes feature updates and writes guarded online feature values to Redis |
| Redis | Stores the latest online feature value per user |
| Replay adapter | Converts selected FeatureForge Parquet records into Kafka-compatible interaction events |

## Data flow

### 1. Source events

FeatureForge produces interaction events containing an event identity, user and
content identifiers, event type, event time, ingestion time, source, and schema
version.

The streaming platform does not modify the source platform. Instead, an explicit
adapter reads supported Parquet records, maps source fields to the streaming
contract, and publishes them to a dedicated replay topic.

Supported source event types are:

- `play`
- `impression`

Source device types are mapped as follows:

| Source device type | Stream source |
|---|---|
| `web` | `web` |
| `android` | `mobile` |
| `ios` | `mobile` |
| `mobile` | `mobile` |
| `tv` | `tv` |

The adapter preserves source event times and ingestion times. It does not
simulate real-time arrival with wall-clock delays.

### 2. Validation

Raw events enter `interaction-events.v1`.

The validation job parses each record against `InteractionEventV1`:

- `event_id` must be a UUID.
- `event_time` and `ingested_at` must be timezone-aware ISO-8601 timestamps.
- Timestamps are normalized to UTC.
- `ingested_at` must be on or after `event_time`.
- `user_id` and `content_id` must be non-empty and contain no whitespace.
- `event_type` must be one of `impression`, `play`, `skip`, or `complete`.
- `source` must be one of `web`, `mobile`, or `tv`.
- `play_seconds` must be a non-negative integer.
- Unknown fields are rejected.
- `impression` events must have `play_seconds = 0`.

Valid records are published to:

```text
interaction-events.validated.v1
```

Invalid records are published to:

```text
interaction-events.rejected.v1
```

A rejected record contains:

```json
{
  "error_schema_version": "v1",
  "reason_code": "INVALID_JSON",
  "raw_payload": "<original payload>",
  "errors": []
}
```

Reason codes:

| Reason code | Meaning |
|---|---|
| `INVALID_JSON` | The payload is not valid JSON |
| `EVENT_CONTRACT_VIOLATION` | The JSON is valid but violates the event contract |

Validation failures are isolated per record. One invalid event does not prevent
subsequent valid events from being processed.

## Event identity

An event is identified by `event_id`.

Two deliveries with the same `event_id` are treated as the same logical event
only when their logical payload is identical. The `ingested_at` field is
excluded from logical identity because it describes delivery or receipt time,
not the business meaning of the interaction.

The logical payload includes:

- `event_id`
- `event_time`
- `user_id`
- `content_id`
- `event_type`
- `play_seconds`
- `source`
- `schema_version`

If the same `event_id` arrives with a different logical payload, the system
raises an `EventIdentityConflictError`. This prevents silently accepting
contradictory data under one identity.

## Event-time semantics

The feature is computed in event time.

For `user_play_count_1h`, the feature window is:

```text
(observation_time - 1 hour, observation_time]
```

This is a right-closed interval:

- An event exactly at `observation_time` belongs to the window.
- An event exactly at `observation_time - 1 hour` does not belong to the window.
- An event after `observation_time` does not belong to the window.

Flink windows are left-closed and right-open:

```text
[start, end)
```

To preserve the reference semantics, event timestamps are mapped before window
assignment:

```python
mapped_timestamp_ms = ceil(event_time_in_microseconds / 1000) - 1
```

This mapping preserves the intended right-closed feature boundary while using
Flink’s native half-open window model.

## Feature definition

### `user_play_count_1h`

`user_play_count_1h` counts the number of unique `play` events for one user
within the preceding hour.

```text
user_play_count_1h(user, observation_time)
= count of unique play events
  where event_time > observation_time - 1 hour
    and event_time <= observation_time
```

Example:

```text
Observation time: 12:00:00Z
Window:           11:00:00Z < event_time <= 12:00:00Z
```

Only `play` events contribute to the count.

`impression`, `skip`, and `complete` events are valid contract events but do not
increase the feature value.

## Windowing

The feature job uses one-hour sliding event-time windows with a one-minute
slide.

```text
Window length: 1 hour
Slide interval: 1 minute
```

This produces a feature update at every whole minute for each user with at
least one qualifying event in the current window.

For example, at `12:00:00Z`, the job computes:

```text
11:00:00Z < event_time <= 12:00:00Z
```

At `12:01:00Z`, it computes:

```text
11:01:00Z < event_time <= 12:01:00Z
```

## Watermarks and late data

The feature job uses a bounded out-of-orderness watermark strategy:

```text
Watermark delay: 10 minutes
Allowed lateness: 30 minutes
```

An event is classified relative to the maximum event time observed before it:

| Condition | Classification |
|---|---|
| `event_time >= max_event_time_seen - watermark_delay` | `ON_TIME` |
| `event_time >= max_event_time_seen - watermark_delay - allowed_lateness` | `ALLOWED_LATE` |
| Otherwise | `TOO_LATE` |

The first observed event is always classified as `ON_TIME`.

A too-late event does not advance the maximum observed event time. This prevents
an old event from permanently suppressing the watermark for subsequent events.

### Late corrections

An allowed-late event can still update a window that has already emitted a
feature value.

Example:

1. A play event arrives for the `11:00–12:00` window.
2. The job emits `user_play_count_1h = 1`.
3. Another valid play event for the same window arrives later.
4. The job emits an updated feature value of `2`.

This is intentional event-time correction, not an error.

### Too-late routing

Once all windows assigned to an event have expired, including their allowed
lateness period, the event can no longer change a feature value.

Such events are written to:

```text
interaction-events.user-play-count-1h.late.v1
```

The late-event record contains:

```json
{
  "error_schema_version": "v1",
  "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
  "feature_name": "user_play_count_1h",
  "raw_payload": "<original event payload>"
}
```

This makes data-loss behavior explicit instead of silently dropping old events.

## Deduplication

The feature job deduplicates events by `event_id`.

Deduplication state uses a 24-hour time-to-live:

```text
DEDUPE_TTL_HOURS = 24
```

A repeated delivery with the same logical payload is ignored. It does not:

- Increase the feature count.
- Extend the deduplication retention period.
- Produce another feature update.

This protects the feature against at-least-once Kafka delivery semantics.

## Feature output contract

Feature updates are published to:

```text
features.user-play-count-1h.v1
```

Each record has this structure:

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

| Field | Meaning |
|---|---|
| `feature_schema_version` | Version of the feature output contract |
| `feature_name` | Stable feature identifier |
| `user_id` | Feature entity |
| `observation_time` | Right-closed end of the feature window |
| `window_start_exclusive` | Exclusive start of the one-hour feature window |
| `play_count` | Number of unique play events in the window |

## Online materialization

The Redis materializer consumes feature updates from Kafka and writes the
latest feature state to Redis.

The Redis key format is:

```text
featureforge-stream:online:v1:user_play_count_1h:<user_id>
```

Example:

```text
featureforge-stream:online:v1:user_play_count_1h:user_000001
```

The stored value is the serialized `UserPlayCountFeatureV1` record.

### Write policy

The materializer applies an append-only, guarded write policy.

| Incoming feature | Stored feature | Result |
|---|---|---|
| Older observation time | Any | `stale` |
| Same observation time, lower count | Any | `stale` |
| Same observation time, equal count | Any | `unchanged` |
| Same observation time, higher count | Any | `written` |
| Newer observation time | Any | `written` |

This policy supports:

- Newer windows replacing older windows.
- Late corrections increasing a count within the same window.
- Repeated deliveries remaining idempotent.
- Replayed older values being rejected.

It does not support retractions or arbitrary feature revisions. A count for the
same window can only increase; an already inflated value cannot be repaired by
this policy.

### Concurrency control

Redis writes use optimistic concurrency control:

1. Watch the feature key.
2. Read the currently stored feature.
3. Apply the write policy.
4. Start a Redis transaction.
5. Write the new feature payload.
6. Execute the transaction.

If another writer changes the key between the read and the transaction, Redis
raises `WatchError`. The materializer retries up to 16 times.

This prevents a stale writer from overwriting a newer concurrent update.

## Replay mode

The system supports continuous processing and bounded historical replay.

| Mode | Purpose |
|---|---|
| Continuous streaming | Process arriving records without a fixed input end |
| Bounded replay | Process a finite input range captured when the stage starts |

### Stage ordering

Both Flink jobs support `--bounded`. Their Kafka sources start at the earliest
available offsets and capture stopping offsets when the source initializes.

For an isolated historical run:

1. Create dedicated replay topics with the timestamp policy below.
2. Publish the complete adapted source dataset.
3. Run validation and wait for `FINISHED`.
4. Verify validated and rejected output ranges.
5. Run feature computation and wait for `FINISHED`.
6. Verify feature and late-event output ranges.
7. Materialize feature output into an isolated Redis prefix.
8. Compare expected latest Kafka feature values with Redis.

Starting a bounded downstream stage before its upstream output is complete can
capture an incomplete range.

### Materializer boundary and commits

The Redis materializer supports one partition. It captures the partition end
offset at startup and starts from the group's committed offset, when available,
or the earliest available offset otherwise.

It rejects a topic with no readable records. For a non-empty range already
consumed by the group, bounded mode returns without processing additional
records.

Kafka offsets are committed after successful Redis processing, including
guarded no-op outcomes (`unchanged` or `stale`). A failure after a Redis write
but before an offset commit can cause that update to be delivered again.

The current implementation has no overall or idle-progress timeout. Bounded
mode defines an offset boundary, not a guaranteed wall-clock completion time.
If records stop arriving before that boundary is reached, polling can continue
indefinitely.

### Kafka timestamps and retention

For local historical replay topics, use:

```text
cleanup.policy=delete
retention.ms=604800000
message.timestamp.type=LogAppendTime
```

Apply this policy when creating replay topics, including feature and late-event
outputs. The seven-day retention is a local demonstration policy, not a
universal production recommendation.

Kafka's record timestamp is transport metadata. With `LogAppendTime`, it is
set from broker append time. JSON timestamps retain their original meanings:

- `event_time`: interaction occurrence time.
- `ingested_at`: original platform input receipt time.
- `observation_time`: feature window observation time.

The current Flink feature job explicitly extracts event time from the payload.
Changing the Kafka record timestamp policy therefore does not change its
declared feature-window membership.

A local replay using `CreateTime` produced historical output timestamps.
Broker logs confirmed that time-based retention deleted the feature and
late-event output before materialization. The validated source remained
readable, so only feature computation needed to be repeated into new output
topics.

Check earliest and latest offsets together. Equal offsets indicate an empty
readable range; an end offset alone is not a retained-record count. Already
deleted records cannot be recovered by changing retention settings.

### Verification boundaries

Comparing latest Kafka feature values with Redis proves materialization
consistency for the checked run. It does not establish batch-stream parity,
checkpoint recovery, or end-to-end exactly-once delivery.

Accelerated replay retains source timestamps and arrival order. Watermark
scheduling can affect late-event admission. Replay ordering alone does not
establish deterministic admission across executions.

## Kafka topology

| Topic | Purpose |
|---|---|
| `interaction-events.v1` | Raw input events |
| `interaction-events.validated.v1` | Contract-valid events |
| `interaction-events.rejected.v1` | Invalid or contract-violating events |
| `features.user-play-count-1h.v1` | Feature updates |
| `interaction-events.user-play-count-1h.late.v1` | Events too late for all assigned windows |
| `interaction-events.replay.<run-name>.v1` | Dedicated replay input topic |

The replay adapter requires a dedicated replay topic beginning with:

```text
interaction-events.replay.
```

It refuses to publish historical records directly to the normal input topic.
This protects the live validation pipeline from accidental historical replays.

## Local infrastructure

Docker Compose provides:

- Kafka in KRaft mode
- Flink JobManager
- Flink TaskManager
- Redis

Kafka is exposed locally on:

```text
127.0.0.1:9092
```

Redis is exposed locally on:

```text
127.0.0.1:6379
```

The Flink UI is exposed locally on:

```text
127.0.0.1:8081
```

Kafka auto-topic creation is disabled. Topics must be created explicitly.

## Testing strategy

The project separates unit correctness from infrastructure integration.

### Unit tests

Unit tests verify:

- Event contract validation
- JSON routing and rejection behavior
- Right-closed window mapping
- Reference feature counting
- Duplicate and conflict semantics
- Arrival-order admission policy
- Redis write guards
- Kafka-to-Redis materialization behavior with fakes

Unit tests do not require Docker, Kafka, or Redis.

### Integration tests

Integration tests verify real behavior across:

- Kafka topics
- Flink jobs
- Redis
- Docker Compose services

They cover:

- Validation routing
- Golden fixture feature values
- Late-event correction
- Duplicate suppression
- Too-late event routing
- Redis feature round trips
- Redis concurrency and retry behavior

Integration tests are excluded from the default pytest run through:

```toml
addopts = "-m 'not integration'"
```

They can be executed explicitly with:

```bash
.venv/bin/python -m pytest -m integration
```

## Correctness boundaries

The current implementation provides strong guarantees for the tested local
system, but it does not claim unlimited production guarantees.

Explicitly supported:

- Event-time feature computation
- At-least-once Kafka delivery with logical deduplication
- Late correction within the allowed-lateness window
- Explicit routing of too-late events
- Guarded Redis writes
- Bounded replay
- Reference parity for the tested feature

Explicitly not claimed:

- Exactly-once end-to-end processing
- Unlimited replay protection beyond the 24-hour deduplication TTL
- Automatic recovery from every Flink failure mode
- Multi-partition Redis materialization
- General-purpose feature revision or retraction semantics
- Historical availability at every past observation time

These boundaries are intentional. The project favors precise, testable
guarantees over vague production claims.

## Module responsibilities

| Module | Responsibility |
|---|---|
| `contracts/events.py` | Defines and validates `InteractionEventV1` |
| `contracts/routing.py` | Separates valid events from structured rejections |
| `contracts/feature_time.py` | Maps event time to Flink window-assignment timestamps |
| `reference/admission.py` | Simplified arrival-order admission reference; not an exact Flink sliding-window simulation |
| `reference/user_play_count.py` | Defines the deterministic feature reference |
| `jobs/validate_and_route_events.py` | Runs the Kafka validation job |
| `jobs/user_play_count_1h.py` | Runs the Flink feature job |
| `materialize.py` | Materializes Kafka feature updates into Redis |
| `online_store.py` | Defines the feature contract and guarded Redis store |
| `replay.py` | Adapts FeatureForge Parquet events for Kafka replay |

## Extension path

The architecture is designed to grow without changing its core principles.

Natural next steps include:

- Adding more event-time features behind the same contract and reference pattern.
- Introducing feature-specific late-event policies.
- Adding offline snapshots and online/offline parity checks.
- Expanding Redis materialization to multiple partitions safely.
- Adding checkpoint-recovery integration tests.
- Introducing schema-registry-based contract governance.
- Adding observability for lag, rejected records, late events, and materialization outcomes.

Each extension should preserve the same rule:

> Define the contract, define the reference, test the semantics, then add the
> infrastructure.