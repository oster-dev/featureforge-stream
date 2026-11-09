# Architecture — FeatureForge Stream

FeatureForge Stream is a local, production-inspired streaming feature platform.

It consumes interaction events from Kafka, validates their contracts, computes
event-time features with Apache Flink, publishes feature updates back to Kafka,
and materializes online feature values into Redis.

The current feature implementations are:

- `user_play_count_1h`
- `user_skip_rate_24h`

The system is deliberately small, explicit, and testable. Its purpose is to
demonstrate trustworthy feature computation under duplicate delivery, late
arrivals, replay, and selected failure scenarios—not production scale or
blanket exactly-once guarantees.

## 1. Implementation and evidence status

Different components have different proof boundaries.

| Capability | Current evidence | Boundary |
|---|---|---|
| Event validation | Contract tests and Flink validation integration tests | Explicitly supported event contract |
| User play count | Python reference and Flink integration tests | Declared one-hour event-time semantics |
| User skip rate | Python reference and eight isolated Flink integration cases | Declared 24-hour windows, late corrections, duplicates, zero denominator, and late routing |
| Play-count online materialization | Kafka-to-Redis verification and Redis tests | Single-partition output and guarded append-only counts |
| Skip-rate online store | Payload/policy tests and twelve real Redis integration cases | Componentwise count ordering within a consistent run namespace |
| Skip-rate materialization | Materializer unit tests and a real Kafka-to-Redis integration test | Controlled feature payloads published directly to Kafka |
| Play-count historical snapshots | Versioned Parquet export and snapshot-to-online verification | Bounded, run-scoped artifact |
| Source-to-snapshot verification | Independent source comparison, coverage checks, and late-output reconciliation | Declared play-count snapshot scope |
| TaskManager recovery | Documented manual play-count recovery drill | Specific local failure scenario |
| Combined skip-rate pipeline | Flink and materialization paths tested separately | No single joined events-to-Flink-to-Redis proof claimed yet |

The presence of a component does not imply that every possible combination
of components has been tested together.

## 2. System overview

```text
FeatureForge Parquet source
            |
            v
   Replay adapter (optional)
            |
            v
Raw interaction-event Kafka topic
            |
            v
+--------------------------------+
| Validation job                 |
| Kafka -> Flink -> Kafka        |
+--------------------------------+
      |                      |
      v                      v
Validated events       Rejected events
      |
      +-------------------------------+
      |                               |
      v                               v
+-------------------------+  +-------------------------+
| user_play_count_1h job  |  | user_skip_rate_24h job  |
| Kafka -> Flink -> Kafka |  | Kafka -> Flink -> Kafka |
+-------------------------+  +-------------------------+
      |            |              |             |
      v            v              v             v
Play-count     Play-count     Skip-rate      Skip-rate
updates        late events    updates        late events
      |                           |
      v                           v
+-------------------------+  +-------------------------+
| Play-count materializer |  | Skip-rate materializer  |
| Kafka -> Redis          |  | Kafka -> Redis          |
+-------------------------+  +-------------------------+
      |                           |
      v                           v
Latest play-count value      Latest skip-rate value
per user                     per user

Play-count Kafka output
      |
      v
Bounded snapshot exporter
      |
      +--> windows.parquet
      |
      +--> verification.json
                  |
                  v
       Snapshot-to-online comparison

Source events + play-count snapshot + recorded late output
                  |
                  v
       Source verification and reconciliation
```

The two feature jobs are separate applications. Their defaults use the same
validated input topic, but each job has its own feature output, late output,
consumer group, and operator identifiers.

Tests and historical runs can override topic names to isolate state and
watermark progression.

## 3. Core design principles

- Event time defines feature-window membership.
- Contracts are validated explicitly.
- Logical identity and receipt metadata are separate concepts.
- Reference correctness and distributed execution are tested separately.
- Late corrections are part of normal feature computation.
- Duplicate suppression has a finite, documented retention horizon.
- Online update rules are feature-specific.
- Independent runs must not silently share incompatible state.
- Every claim is bounded by its actual test or recorded drill.
- Existing FeatureForge Batch artifacts remain owned by Project 1.

## 4. Runtime components

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

## 5. Source integration and project ownership

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

Source device mapping:

| Source device type | Stream source |
|---|---|
| `web` | `web` |
| `android` | `mobile` |
| `ios` | `mobile` |
| `mobile` | `mobile` |
| `tv` | `tv` |

The adapter preserves source event and ingestion timestamps. It does not
simulate real-time arrival through wall-clock delays.

Historical publication requires a dedicated topic beginning with:

```text
interaction-events.replay.
```

The adapter refuses to publish historical records directly to the normal
live input topic.

## 6. Event contract and validation

Default raw input:

```text
interaction-events.v1
```

The validation job parses records as `InteractionEventV1`.

Rules include:

- `event_id` is a UUID.
- `event_time` and `ingested_at` are timezone-aware timestamps.
- Timestamp strings contain an ISO-8601 datetime with a `T` separator.
- Timestamps are normalized to UTC.
- `ingested_at >= event_time`.
- Entity IDs are non-empty strings without whitespace.
- `event_type` is `impression`, `play`, `skip`, or `complete`.
- `source` is `web`, `mobile`, or `tv`.
- `play_seconds` is a strict non-negative integer.
- `schema_version` is `v1`.
- Unknown fields are rejected.
- Impression events have `play_seconds = 0`.

Default valid output:

```text
interaction-events.validated.v1
```

Default rejected output:

```text
interaction-events.rejected.v1
```

Rejected records preserve the original payload and structured validation
information:

```json
{
  "error_schema_version": "v1",
  "reason_code": "INVALID_JSON",
  "raw_payload": "<original payload>",
  "errors": []
}
```

| Reason code | Meaning |
|---|---|
| `INVALID_JSON` | Payload cannot be parsed as valid JSON |
| `EVENT_CONTRACT_VIOLATION` | JSON violates the declared event contract |

Validation failures are isolated per record.

A feature job also parses its input against `InteractionEventV1`, but this is
not a replacement for the validation job's structured rejection routing.
Feature jobs normally consume validated input.

## 7. Logical event identity

Logical identity is `event_id`.

Two deliveries with the same ID represent the same logical event only when
their logical payloads match.

Logical payload includes:

- Event identity
- Event time
- User and content IDs
- Event type
- Playback duration
- Source
- Schema version

`ingested_at` is excluded because it represents receipt metadata.

Conflicting payloads for one ID raise `EventIdentityConflictError`.

The Python references validate identities over the complete supplied input
before applying user, event-type, or time filters.

Streaming jobs can detect conflicts only while the relevant deduplication
state is retained. An identity conflict is a job error, not a normal
validation-rejection record.

## 8. Time semantics

### Event time versus availability

Window membership uses `event_time`.

`ingested_at` remains separate receipt metadata. The Python references can
include an event whose receipt timestamp is later than the observation time.

These references reconstruct event-time values from supplied records.
They do not establish what an online consumer could have known at a
historical observation time.

### Right-closed windows

Both features use:

```text
(observation_time - window_length, observation_time]
```

The lower boundary is excluded; the upper boundary is included.

Flink's native windows use:

```text
[start, end)
```

The timestamp assignment maps original event times as follows:

```python
mapped_timestamp_ms = ceil(event_time_in_microseconds / 1000) - 1
```

For whole-millisecond window boundaries, this preserves the declared
right-closed membership.

The mapped value is a window-assignment timestamp, not a replacement for
the original payload timestamp.

## 9. Feature definitions

### user_play_count_1h

Count unique `play` events for one user within:

```text
(observation_time - 1 hour, observation_time]
```

Other valid event types do not increment the count.

The Python reference returns an integer. Empty eligible input returns zero.

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
- A zero denominator produces `0.0`.
- Impression and complete events contribute to neither count.
- Output rate is a float in `[0.0, 1.0]`, not a percentage.
- Content IDs do not partition this user-level feature.
- No pairing between plays and skips is inferred.

This is a ratio of event counts, not a session-level skip probability.

The output retains both counts so that consumers can validate the rate and
apply the declared update policy.

## 10. Window execution

| Feature | Window length | Slide |
|---|---|---|
| `user_play_count_1h` | 1 hour | 1 minute |
| `user_skip_rate_24h` | 24 hours | 1 minute |

Observation times are minute-aligned.

Windows emit when event-time progress permits, not simply because wall-clock
time reaches the next minute. Allowed-late events can cause further emissions
for an already emitted window.

All accepted event types enter window aggregation. A window containing only
non-contributing event types can therefore emit a zero result.

The jobs do not create windows for users with no input events. They do not
provide an indefinite wall-clock heartbeat after input stops.

### Local state-cost boundary

With a one-minute slide:

- A one-hour window assigns an event to 60 windows.
- A 24-hour window assigns an event to 1,440 windows.

The skip-rate configuration prioritizes a small, understandable correctness
demonstration. It is not a throughput benchmark or a demonstrated
production-scale state design.

Aggregators retain counts rather than full event lists per window.
Deduplication state is maintained separately.

## 11. Watermarks and late-event admission

Both feature jobs configure:

```text
Bounded out-of-orderness: 10 minutes
Allowed lateness:        30 minutes
```

Watermarks describe event-time progress. They are affected by observed
timestamps, source/channel progress, and runtime watermark emission.

Allowed lateness applies to individual assigned windows. An event may be
too late for an earlier assigned window while still being accepted by
another overlapping window.

An event is routed to the feature's late side output when all its assigned
windows are too late.

A single event-age classification relative to a maximum timestamp is not
an exact simulation of this sliding-window behavior.

### Admission reference boundary

`reference/admission.py` provides a simplified arrival-order policy used by
verification and reconciliation.

It must not be described as the exact live Flink watermark scheduler or as
a complete model of per-window admission.

Accelerated replay preserves source timestamps, but runtime watermark
scheduling can affect which late records are admitted. Arrival order alone
does not prove identical admission across executions.

### Late corrections

Play counts can increase when an additional play is accepted for an
already emitted window.

Skip rates can move in either direction:

| Accepted same-window state | Plays | Skips | Rate |
|---|---:|---:|---:|
| Initial | 1 | 1 | 0.5 |
| Additional play | 2 | 1 | 1/3 |
| Additional skip | 2 | 2 | 0.5 |

A lower rate is not necessarily a stale update.

### Late outputs

| Feature | Default late topic |
|---|---|
| Play count | `interaction-events.user-play-count-1h.late.v1` |
| Skip rate | `interaction-events.user-skip-rate-24h.late.v1` |

Example:

```json
{
  "error_schema_version": "v1",
  "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
  "feature_name": "user_skip_rate_24h",
  "raw_payload": "<original event payload>"
}
```

## 12. Deduplication and state retention

Both jobs key deduplication state by `event_id`.

| Job | Deduplication TTL |
|---|---|
| Play count | 24 hours |
| Skip rate | 48 hours |

TTL uses processing time, not event time.

The first accepted delivery stores the logical payload. Identical repeated
deliveries are suppressed and do not extend retention.

After TTL expiry, the same ID may be accepted again. This can occur even
while an event-time window remains open if watermark progress has stalled.

The TTL is therefore a finite duplicate/conflict-detection horizon, not a
proof of unlimited replay safety.

Each job has independent state. Running both features does not create a
shared global deduplication service.

## 13. Feature output contracts

### Play-count output

Default topic:

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

### Skip-rate output

Default topic:

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

The skip-rate payload model validates:

- Strict non-negative integer counts
- A finite rate within `[0.0, 1.0]`
- Rate consistency with counts, within the declared numerical tolerance
- Exactly 24 hours between window boundaries
- Timezone-aware timestamps normalized to UTC
- Feature name and schema version
- Entity-ID syntax
- Rejection of unknown fields

Both jobs use at-least-once Kafka sinks. Duplicate output records are possible.

## 14. Online materialization

Redis stores the latest accepted observation window per user in a
feature-specific namespace.

These are latest-value stores, not historical window stores.

### Play-count policy

Default key format:

```text
featureforge-stream:online:v1:user_play_count_1h:<user_id>
```

Stored model: `UserPlayCountFeatureV1`.

| Incoming value | Decision |
|---|---|
| Newer observation window | `written` |
| Older observation window | `stale` |
| Same window, larger count | `written` |
| Same window, equal count | `unchanged` |
| Same window, smaller count | `stale` |

This policy is specific to append-only counts without retractions.
It cannot repair an already inflated count.

### Skip-rate policy

Stored model: `UserSkipRateFeatureV1`.

The skip-rate materializer requires an explicit `--key-prefix`.
Independent runs require independent namespaces.

Within the same window, compare:

```text
(play_count, skip_count)
```

| Incoming value | Decision |
|---|---|
| Newer observation window | `written` |
| Older observation window | `stale` |
| Same window, identical counts | `unchanged` |
| Both counts greater than or equal, at least one greater | `written` |
| Both counts less than or equal, at least one smaller | `stale` |
| One count increases and the other decreases | Raise `SkipRateUpdateConflictError` |

The policy does not compare rates by magnitude.

It does not merge conflicting records by taking componentwise maxima,
because that could manufacture a result the job never emitted.

Componentwise ordering assumes consistent append-only computation within
one run namespace. It does not prove that two independent computations used
the same event set.

Retracted events, downward count corrections, and repair of inflated counts
are unsupported.

### Concurrency control

Both stores use Redis optimistic concurrency:

1. Watch the user key.
2. Read and validate its current payload.
3. Apply the feature-specific policy.
4. For an accepted write, execute `MULTI/SET/EXEC`.
5. Retry after `WatchError`, reading the current value again.

Retries are bounded at 16 attempts. Connection errors, invalid stored payloads,
policy conflicts, and exhausted retries propagate to the caller.

A real Redis test exercises a competing same-window skip-rate write and
verifies that the retry preserves the more advanced count vector.

## 15. Materializer offsets and failure boundaries

Materializers currently support exactly one Kafka partition.

They inspect the retained low/high offsets and start from the group's
committed offset when available, otherwise from the retained low offset.
A start offset below the retained low offset is advanced to that low offset.

In bounded mode, the captured high offset is the exclusive stopping boundary.

Behavior:

- An empty readable range is rejected.
- An already caught-up group returns without writes.
- Tombstones and invalid feature payloads are rejected.
- Redis processing precedes offset commit.
- Successful no-op outcomes are processed records and can be committed.
- Commits use the next offset and are synchronous.
- A failed record is not committed as successfully processed.

A Redis write and a Kafka offset commit are not one distributed transaction.
A failure between them can cause redelivery.

The skip-rate materializer disables both automatic commits and automatic
offset storage.

There is no overall or idle-progress timeout. Bounded mode defines an
offset boundary, not a wall-clock completion guarantee.

### Proof boundaries

Play-count Kafka-to-Redis consistency has a separate verification path.

Skip-rate Kafka-to-Redis integration checks:

- Decreasing and increasing rates
- Stored Redis values after each phase
- Stale and unchanged updates
- Newer zero-valued windows
- Committed offsets read through a separate consumer
- Caught-up execution without writes
- Replay of the same output with a fresh consumer group

The skip-rate integration publishes controlled feature payloads directly
to Kafka. A combined interaction-events-to-Flink-to-Redis execution is not
claimed by that test.

## 16. Historical replay

The validation job and both feature jobs support `--bounded`.

Sources start at the earliest available offsets and capture stopping offsets
during source initialization.

For an isolated staged replay:

1. Create dedicated topics.
2. Publish the complete adapted dataset.
3. Run validation and wait for `FINISHED`.
4. Verify validated/rejected output.
5. Run the selected feature job and wait for `FINISHED`.
6. Verify feature and late output.
7. Materialize into an isolated Redis namespace.
8. Run the applicable consistency checks.

Starting a bounded downstream stage too early can capture an incomplete
upstream range.

Fresh topics and namespaces do not by themselves establish deterministic
late-event admission; watermark scheduling remains relevant.

### Kafka timestamps and retention

The declared local historical replay-topic policy is:

```text
cleanup.policy=delete
retention.ms=604800000
message.timestamp.type=LogAppendTime
```

This policy applies to explicitly configured historical replay topics.
It is not a claim that every integration-test topic uses these settings.

Kafka record timestamps are transport metadata. JSON event, receipt, and
observation timestamps retain their separate meanings.

Feature jobs extract event time from payloads. `LogAppendTime` therefore
does not change declared feature-window membership.

A previous local replay demonstrated historical output deletion under
time-based retention with `CreateTime`. Broker logs confirmed the deletion.

Inspect low and high offsets together:

- Equal offsets mean no readable range.
- A high offset alone is not a retained-record count.
- Changing retention cannot restore deleted records.

## 17. Play-count snapshots

Snapshot support currently applies to `user_play_count_1h`.

A completed export contains:

```text
windows.parquet
verification.json
```

Contract identifiers:

```text
featureforge-stream.user-play-count-1h.snapshot.v1
featureforge-stream.replay-snapshot-export.v1
```

The snapshot is not Project 1's canonical offline store and is not
schema-compatible with its existing engagement feature tables.

No automatic Feast or Project 3 compatibility is claimed.

### Row semantics

Rows contain the declared play-count feature fields.

Logical key:

```text
(user_id, observation_time)
```

Rows are sorted by that key. Repeated same-window updates are consolidated
using maximum `play_count`.

This is valid only for the declared append-only count model.
It cannot represent downward revisions or repair inflated counts.

Different observation windows remain separate rows.

This consolidation policy must not be reused as maximum `skip_rate`.
Skip-rate snapshot support is not implemented by this exporter.

### Export prerequisites

The declared export requires:

- Completed upstream replay and materialization
- A quiescent Kafka output and Redis namespace
- One Kafka partition
- A non-empty retained range beginning at offset zero
- Contiguous offsets and no tombstones
- Stable Kafka low/high offsets during reading
- A new output directory

The exporter records offsets, row counts, contract metadata, and a Parquet
SHA-256. It reads back and validates the persisted artifact before comparison.

The latest persisted window per user is compared with Redis, including a
check for additional namespace keys.

Redis reads and namespace scanning are not an atomic database snapshot.

### Acceptance and publication

An accepted export requires a valid `verification.json`, matching Parquet
hash, and `comparison.passed = true`.

The Parquet file and report are not published as one crash-atomic unit.
Interrupted or failed exports can leave incomplete directories.

The exporter holds bounded replay data in memory. It is not a continuous
offline sink or a demonstrated large-scale writer.

Snapshot-to-online consistency does not establish historical training
availability or leakage-safe retrieval.

Detailed contract:

```text
docs/contracts/user-play-count-1h-snapshot-v1.md
```

## 18. Source verification and late reconciliation

Source-to-snapshot checks provide an additional proof beyond comparing
Redis with Kafka-derived output.

The verification path compares source-derived play-count expectations with
the declared snapshot windows, checks coverage, and reports differences.

Recorded late output can then be used to reconcile observed exclusions under
the declared verification policy.

The completed local evidence includes source-to-snapshot coverage and
separate reconciliation using verified late events.

This is not a Spark-versus-Flink parity suite. It does not prove arbitrary
batch-stream equivalence, complete historical training safety, or exact
simulation of Flink's runtime admission behavior.

Detailed contract:

```text
docs/contracts/source-snapshot-verification-v1.md
```

## 19. Checkpoints and recovery evidence

Both feature jobs enable checkpointing at a 10-second interval.

Checkpoint configuration alone does not prove successful recovery or
end-to-end exactly-once processing.

### Completed manual play-count drill

A controlled local drill:

1. Kept the JobManager and Kafka running.
2. Killed and restarted the TaskManager.
3. Observed the same job restore from checkpoint `23`.
4. Repeated an original play and added a new play.
5. Observed the reference count `2`.
6. Observed no excessive count during the following 15 seconds.

The drill is documented in:

```text
docs/runbooks/taskmanager-checkpoint-recovery.md
```

This is manual evidence for a specific TaskManager-failure scenario.

It is not:

- An automated recovery suite
- A JobManager or full-cluster disaster-recovery proof
- A skip-rate recovery proof
- An end-to-end exactly-once guarantee

Checkpoint durability across broader container/host failures must not be
inferred from this drill.

## 20. Kafka topology

| Default topic | Purpose |
|---|---|
| `interaction-events.v1` | Raw events |
| `interaction-events.validated.v1` | Validated events |
| `interaction-events.rejected.v1` | Structured validation failures |
| `features.user-play-count-1h.v1` | Play-count updates |
| `interaction-events.user-play-count-1h.late.v1` | Play-count late output |
| `features.user-skip-rate-24h.v1` | Skip-rate updates |
| `interaction-events.user-skip-rate-24h.late.v1` | Skip-rate late output |
| `interaction-events.replay.<run-name>.v1` | Dedicated historical replay input |

Job arguments support isolated input/output topic overrides.

## 21. Local infrastructure and deployment

Docker Compose provides:

- Kafka in KRaft mode
- Flink JobManager
- Flink TaskManager
- Redis with append-only persistence enabled

Local endpoints:

| Service | Endpoint |
|---|---|
| Kafka | `127.0.0.1:9092` |
| Flink UI / REST | `127.0.0.1:8081` |
| Redis | `127.0.0.1:6379` |

Kafka's internal container endpoint is `kafka:29092`.

Auto-topic creation is disabled.

The TaskManager is configured with two task slots.
Feature jobs use parallelism one.

### Image and code distribution

The custom Flink image contains:

- Flink 1.20.3
- Python 3.11
- PyFlink
- Kafka connector
- The installed project package

The host `jobs/` directory is mounted read-only into Flink containers.

Changing a mounted job script does not by itself require an image rebuild.
Changes to installed `src/` package code require rebuilding/redeploying the
image when that new package code is needed inside Flink containers.

The current Redis materializers run through the local Python environment
in the demonstrated workflows.

### Security boundary

This is a local development stack, not a hardened production deployment.

Kafka uses plaintext listeners. The Compose configuration does not establish
production authentication, authorization, encrypted service traffic, or
managed secret handling.

Local port binding reduces exposure but is not a substitute for a production
security design.

## 22. Testing strategy

### Standard local checks

```bash
make check
```

The standard suite runs lint, formatting checks, and tests excluding the
`integration` marker.

Unit tests cover:

- Event and feature payload contracts
- JSON routing
- Right-closed timestamp mapping
- Deterministic feature references
- Identity conflicts
- Simplified admission and reconciliation
- Snapshot semantics and verification
- Skip-rate count-vector ordering
- Materializer behavior with fakes

A green standard run does not mean infrastructure integration tests ran.

### Explicit integration checks

Examples:

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_user_skip_rate_job.py -v -s -x
```

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_redis_skip_rate_store.py -v -x
```

```bash
.venv/bin/python -m pytest \
  -m integration \
  tests/integration/test_skip_rate_materialization.py -v -s -x
```

Integration tests have different prerequisites. Some use existing jobs;
others create isolated topics and submit their own job.

The entire integration directory must not be described as a one-command,
self-provisioning suite without verifying those prerequisites.

### Skip-rate Flink evidence

The eight isolated cases cover:

- Late corrections and duplicate safety
- One microsecond before the lower boundary
- The excluded lower boundary
- One microsecond inside the lower boundary
- The included upper boundary
- One microsecond after the upper boundary
- Zero denominator with non-contributing events
- Too-late routing after all assigned windows expire

Each case creates dedicated topics and submits its own job.
The cleanup path requests cancellation of the identified job.

Topics are retained for diagnosis.

### Redis and materializer evidence

Real Redis tests cover:

- Round trips and missing users
- Increasing and decreasing rates
- Window ordering and zero replacement
- Idempotent/stale updates
- Incomparable count conflicts
- Invalid stored payload protection
- A real concurrent-write conflict and retry

Materializer tests separately cover fake-client control flow and real
Kafka-to-Redis operation.

Test counts belong to dated verification output rather than permanent
architecture guarantees.

A successful GitHub check must not be interpreted as proof that every
separately executed integration test ran in CI.

## 23. Module responsibilities

| Module or path | Responsibility |
|---|---|
| `contracts/events.py` | Validates interaction events |
| `contracts/routing.py` | Structured validation routing |
| `contracts/feature_time.py` | Right-closed timestamp mapping |
| `contracts/user_skip_rate.py` | Skip-rate payload and update decision |
| `reference/user_play_count.py` | Play-count reference and shared identity error |
| `reference/user_skip_rate.py` | Skip-rate reference |
| `reference/admission.py` | Simplified arrival-order admission policy |
| `reference/snapshot_parity.py` | Source/snapshot reference comparison |
| `reference/reconciliation.py` | Late-output reconciliation |
| `jobs/validate_and_route_events.py` | Validation job |
| `jobs/user_play_count_1h.py` | Play-count job |
| `jobs/user_skip_rate_24h.py` | Skip-rate job |
| `online_store.py` | Play-count payload and Redis store |
| `skip_rate_store.py` | Skip-rate Redis store |
| `materialize.py` | Play-count materializer |
| `materialize_skip_rate.py` | Skip-rate materializer |
| `replay.py` | Project 1 source adapter |
| `snapshot.py` | Play-count snapshot support |
| `scripts/export_replay_snapshot.py` | Snapshot export command |
| `scripts/verify_kafka_redis.py` | Play-count Kafka/Redis verification |
| `scripts/verify_source_snapshot.py` | Source/snapshot verification |
| `scripts/reconcile_source_snapshot.py` | Reconciliation command |
| `scripts/create_replay_topics.py` | Guarded replay-topic setup |

Some feature-specific pipeline structure is intentionally duplicated.
A general feature compiler or universal sink abstraction is not claimed.

## 24. Explicit correctness boundaries

Supported within the demonstrated scope:

- Validated event contracts
- Right-closed event-time windows
- Finite-horizon logical deduplication
- Tested late corrections
- Explicit all-windows-too-late routing
- Feature-specific guarded Redis writes
- Single-partition materialization
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
- A combined skip-rate events-to-Flink-to-Redis proof
- Multi-partition materialization
- Production-scale performance or high availability
- Deployed monitoring, schema-registry governance, or AWS production operation
- A deployed Project 3 training workflow

## 25. Remaining extension path

Next correctness and product work includes:

1. A combined skip-rate events-to-Flink-to-Redis verification.
2. A declared skip-rate snapshot contract and tested consolidation policy.
3. Additional feature implementations with explicit references.
4. A leakage-safe label contract and training handoff.
5. Declared batch-stream parity over matching feature/time semantics.
6. Practitioner-facing specs, CLI, generated catalog, and notebook.
7. Operational metrics, dashboards, thresholds, and additional runbooks.
8. Broader failure/recovery evidence where useful.
9. Reproducible clean-clone demo and release documentation.
10. An honest local-to-AWS production mapping.

Each extension follows the same rule:

> Define the contract, define the reference, test the semantics,
> then connect and verify the infrastructure.