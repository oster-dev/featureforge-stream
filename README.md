# FeatureForge Stream

FeatureForge Stream is a local, production-inspired streaming feature engine
built with Kafka, Apache Flink, and Redis.

It validates interaction events, computes event-time features, handles late
and duplicate events explicitly, publishes feature updates to Kafka, and
materializes online values into Redis using feature-specific write policies.

## Project status: v0.1.0 — Streaming Engine Prototype

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

## What this project demonstrates

- Strict event and feature contracts
- Kafka-based event transport
- Stateful Flink event-time computation
- Right-closed event-time windows
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

## Implemented features

| Feature | Window | Output |
|---|---|---|
| `user_play_count_1h` | Trailing 1 hour | Unique play count |
| `user_skip_rate_24h` | Trailing 24 hours | Play count, skip count, and skip rate |

Both features use event time, not Kafka arrival time.

### user_play_count_1h

Counts unique play events for one user within:

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

Example:

```text
1 play + 1 skip -> 0.5
2 plays + 1 skip -> 1/3
3 plays + 2 skips -> 0.4
```

## Architecture

```text
FeatureForge Parquet source
        |
        v
Replay adapter
        |
        v
Raw interaction-event topic
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

## Kafka topics

| Topic | Purpose |
|---|---|
| `interaction-events.v1` | Raw input events |
| `interaction-events.validated.v1` | Contract-valid events |
| `interaction-events.rejected.v1` | Invalid JSON or contract violations |
| `features.user-play-count-1h.v1` | Play-count updates |
| `features.user-skip-rate-24h.v1` | Skip-rate updates |
| `interaction-events.*.late.v1` | Events too late for all assigned windows |
| `interaction-events.replay.<run-name>.v1` | Dedicated historical replay input |

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

Validation rules:

- `event_id` must be a UUID.
- Timestamps must be timezone-aware and are normalized to UTC.
- `ingested_at >= event_time`.
- `user_id` and `content_id` must be non-empty.
- `event_type` must be `impression`, `play`, `skip`, or `complete`.
- `play_seconds` must be a non-negative integer.
- `source` must be `web`, `mobile`, or `tv`.
- `schema_version` must be `v1`.
- Unknown fields are rejected.

Detailed contracts:

- [Interaction Events V1](docs/contracts/interaction-events-v1.md)
- [User Play Count 1h V1](docs/contracts/user-play-count-1h-v1.md)
- [User Skip Rate 24h V1](docs/contracts/user-skip-rate-24h-v1.md)

## Event-time behavior

| Feature | Window | Slide |
|---|---|---|
| `user_play_count_1h` | 1 hour | 1 minute |
| `user_skip_rate_24h` | 24 hours | 1 minute |

Both use right-closed windows:

```text
(observation_time - window_length, observation_time]
```

Current configuration:

```text
Bounded out-of-orderness: 10 minutes
Allowed lateness: 30 minutes
```

With sliding windows, an event can be too late for one assigned window while
still being accepted by another. Only an event too late for all assigned
windows is sent to the late output.

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

Redis holds the latest accepted observation window per user. It is not a
historical window store.

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

The store does not select the largest rate or invent a merged count vector.

Both stores use `WATCH`, `MULTI`, and `EXEC`. If the key changes before a
transaction commits, the store retries with a fresh read and reapplies the
policy. Retries are limited to 16 attempts.

## Local setup

Requirements:

- Python 3.11+
- Docker and Docker Compose
- Make

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,replay]"
make check
docker compose up -d --build
```

Services:

| Service | Endpoint |
|---|---|
| Kafka | `127.0.0.1:9092` |
| Flink UI / REST | `127.0.0.1:8081` |
| Redis | `127.0.0.1:6379` |

Stop the stack:

```bash
docker compose down
```

The stack is not hardened for production. Kafka uses plaintext listeners and
does not establish production authentication, authorization, encryption, or
secret management.

## Tests

Run default checks:

```bash
make test
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

## Historical replay

Preview an adapted dataset:

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

For isolated replay, use dedicated topics and a unique Redis namespace.
Independent runs require independent namespaces.

## Snapshot and reconciliation

Export a play-count replay snapshot:

```bash
.venv/bin/python scripts/export_replay_snapshot.py \
  --topic "<replay-feature-topic>" \
  --key-prefix "<replay-redis-key-prefix>" \
  --output-dir "output/stream_snapshots/<unique-export-id>" \
  --timeout-seconds 180
```

The exporter creates:

```text
windows.parquet
verification.json
```

Compare source events with a persisted snapshot:

```bash
.venv/bin/python scripts/verify_source_snapshot.py \
  --input-topic "<validated-replay-topic>" \
  --snapshot-dir "<existing-snapshot-directory>" \
  --report "<new-report-path>.json"
```

Reconcile observed late exclusions:

```bash
.venv/bin/python scripts/reconcile_source_snapshot.py \
  --input-topic "<validated-replay-topic>" \
  --late-topic "<matching-feature-run-late-topic>" \
  --snapshot-dir "<existing-snapshot-directory>" \
  --report "<new-reconciliation-report-path>.json"
```

`RECONCILED` means consistency with observed exclusions. It is not independent
proof of watermark decisions, Spark batch parity, historical training safety,
or recovery.

## TaskManager recovery

A controlled local play-count drill killed and restarted the TaskManager while
keeping the JobManager and Kafka running.

The same job restored from checkpoint `23`. Repeating an original play and
adding a new play produced the reference count `2`, with no excessive count
observed during the next 15 seconds.

This is a documented manual drill, not an automated recovery suite, a
skip-rate recovery proof, or an end-to-end exactly-once guarantee.

See
[TaskManager Checkpoint Recovery](docs/runbooks/taskmanager-checkpoint-recovery.md).

## Project structure

```text
featureforge-stream/
├── jobs/
├── src/featureforge_stream/
├── scripts/
├── tests/
├── fixtures/
├── docs/
├── docker/
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
- A single combined validation-to-Redis or three-project integration proof
- Recovery from every failure mode
- Production-scale performance, security, or availability
- A deployed downstream ML workflow

## Roadmap alignment

FeatureForge Stream is the streaming-engine prototype within a connected
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