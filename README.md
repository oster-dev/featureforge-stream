# FeatureForge Stream

**FeatureForge Stream** is a local streaming feature platform built with **Kafka**, **Apache Flink**, and **Redis**.

It validates interaction events, computes an event-time feature called `user_play_count_1h`, handles late and duplicate events explicitly, publishes feature updates to Kafka, and materializes the latest online feature value into Redis.

The project is part of the broader FeatureForge system:

```text
FeatureForge (batch/source platform)
        |
        v
FeatureForge Stream (streaming feature platform)
        |
        v
ML workflow / feature consumption
```

## What this project demonstrates

FeatureForge Stream demonstrates practical Data & Feature Infrastructure engineering:

- Strict event contracts and input validation
- Kafka-based stream processing
- Apache Flink event-time windows
- Watermarks, allowed lateness, and too-late routing
- Duplicate suppression by logical event identity
- Deterministic feature reference implementation
- Kafka-to-Redis materialization
- Guarded online feature writes
- Bounded historical replay
- Unit and Docker-based integration testing

This is not a generic demo project. Every important behavior has an explicit contract, a reference implementation, and tests.

## Feature: `user_play_count_1h`

`user_play_count_1h` counts the number of unique `play` events for one user within the preceding hour.

```text
user_play_count_1h(user, observation_time)
= count of unique play events
  where observation_time - 1 hour < event_time
    and event_time <= observation_time
```

The feature window is right-closed:

```text
(observation_time - 1 hour, observation_time]
```

Example:

```text
Observation time: 2026-01-01T12:00:00Z
Feature window:   2026-01-01T11:00:00Z < event_time <= 2026-01-01T12:00:00Z
```

Only `play` events increase the feature value.

`impression`, `skip`, and `complete` events are valid input events, but they do not contribute to `user_play_count_1h`.

## Architecture

```text
FeatureForge Parquet
        |
        v
Replay adapter
        |
        v
interaction-events.v1
        |
        v
+----------------------+
| Validation job       |
| Kafka -> Flink       |
+----------------------+
     |              |
     v              v
validated       rejected
events          events
     |
     v
+----------------------+
| Feature job          |
| Kafka -> Flink       |
+----------------------+
     |              |
     v              v
feature         too-late
updates         events
     |
     v
+----------------------+
| Redis materializer   |
| Kafka -> Redis       |
+----------------------+
     |
     v
Online feature lookup
```

## Kafka topics

| Topic | Purpose |
|---|---|
| `interaction-events.v1` | Raw input events |
| `interaction-events.validated.v1` | Valid events after contract validation |
| `interaction-events.rejected.v1` | Invalid JSON or contract violations |
| `features.user-play-count-1h.v1` | Feature updates |
| `interaction-events.user-play-count-1h.late.v1` | Events too late for all assigned windows |
| `interaction-events.replay.<run-name>.v1` | Dedicated historical replay input |

## Event contract

The input contract is `interaction-events.v1`.

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
| `event_id` | Must be a UUID |
| `event_time` | Must be a timezone-aware ISO-8601 timestamp |
| `ingested_at` | Must be a timezone-aware ISO-8601 timestamp |
| `user_id` | Non-empty string without whitespace |
| `content_id` | Non-empty string without whitespace |
| `event_type` | One of `impression`, `play`, `skip`, `complete` |
| `play_seconds` | Non-negative integer |
| `source` | One of `web`, `mobile`, `tv` |
| `schema_version` | Must be `v1` |

Additional rules:

- Timestamps are normalized to UTC.
- `ingested_at` must be on or after `event_time`.
- `impression` events must have `play_seconds = 0`.
- Unknown fields are rejected.
- Invalid JSON is routed to the rejected topic.
- Contract violations are routed to the rejected topic.

## Rejected records

Invalid records are published to:

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

Reason codes:

| Reason code | Meaning |
|---|---|
| `INVALID_JSON` | The payload is not valid JSON |
| `EVENT_CONTRACT_VIOLATION` | The payload is valid JSON but violates the event contract |

Validation failures are isolated per record. One invalid event does not prevent later valid events from being processed.

## Event-time behavior

The feature is computed from `event_time`, not from Kafka arrival time or `ingested_at`.

The feature job uses:

```text
Watermark delay: 10 minutes
Allowed lateness: 30 minutes
```

| Event condition | Classification |
|---|---|
| Within the watermark delay | `ON_TIME` |
| Beyond the watermark delay but within allowed lateness | `ALLOWED_LATE` |
| Beyond allowed lateness | `TOO_LATE` |

Allowed-late events can correct an already emitted feature window.

Example:

```text
1. A play event arrives for the 11:00-12:00 window.
2. The feature job emits play_count = 1.
3. Another valid play event for the same window arrives late.
4. The feature job emits play_count = 2.
```

Events that are too late for every assigned window are routed to:

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

## Duplicate handling

Events are deduplicated by `event_id`.

A repeated delivery with the same logical payload does not increase the feature count.

The logical payload excludes `ingested_at`, because ingestion time describes delivery, not the business meaning of the event.

If the same `event_id` arrives with a different logical payload, the system raises an identity conflict error.

## Feature output

Feature updates are written to:

```text
features.user-play-count-1h.v1
```

Example:

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

## Redis online store

The Redis materializer writes the latest feature value for each user.

Redis key format:

```text
featureforge-stream:online:v1:user_play_count_1h:<user_id>
```

Example:

```text
featureforge-stream:online:v1:user_play_count_1h:user_000001
```

The materializer applies guarded writes:

| Incoming feature | Stored feature | Result |
|---|---|---|
| Older observation time | Any | `stale` |
| Same observation time, lower count | Any | `stale` |
| Same observation time, equal count | Any | `unchanged` |
| Same observation time, higher count | Any | `written` |
| Newer observation time | Any | `written` |

This prevents an older replayed feature value from overwriting a newer online value.

Redis writes use optimistic concurrency control with `WATCH`, `MULTI`, and `EXEC`. If a concurrent writer changes the key, the materializer retries up to 16 times.

## Local setup

### Requirements

- Python 3.11+
- Docker and Docker Compose
- Make
- A local Python virtual environment

### Create the environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,replay]"
```

### Run quality checks

```bash
make check
```

This runs:

```bash
ruff check .
ruff format --check .
pytest -q
```

### Start the local stack

```bash
docker compose up -d --build
```

This starts:

- Kafka
- Flink JobManager
- Flink TaskManager
- Redis

### Check service health

```bash
docker compose ps
```

Expected services:

```text
kafka
jobmanager
taskmanager
redis
```

### Stop the stack

```bash
docker compose down
```

To remove persisted Kafka and Redis data:

```bash
docker compose down -v
```

## Running tests

### Unit tests

Unit tests do not require Docker, Kafka, or Redis.

```bash
make test
```

Or directly:

```bash
.venv/bin/python -m pytest -q
```

### Integration tests

Integration tests require the local Docker Compose stack.

```bash
.venv/bin/python -m pytest -m integration
```

They verify real Kafka routing, Flink feature computation, late-event behavior, too-late routing, and Redis materialization.

## Running the validation job

The validation job reads raw events and routes them to validated or rejected topics.

```bash
.venv/bin/python -m featureforge_stream.jobs.validate_events \
  --bootstrap-servers kafka:29092
```

When running from the host, use:

```bash
.venv/bin/python -m featureforge_stream.jobs.validate_events \
  --bootstrap-servers 127.0.0.1:9092
```

## Running the feature job

The feature job computes `user_play_count_1h` from validated events.

```bash
.venv/bin/python -m featureforge_stream.jobs.user_play_count \
  --bootstrap-servers kafka:29092
```

When running from the host, use:

```bash
.venv/bin/python -m featureforge_stream.jobs.user_play_count \
  --bootstrap-servers 127.0.0.1:9092
```

## Materializing features into Redis

```bash
.venv/bin/python -m featureforge_stream.materialize \
  --bootstrap-servers 127.0.0.1:9092 \
  --redis-url redis://127.0.0.1:6379/0 \
  --group-id featureforge-stream-redis-materializer-v1
```

For an isolated bounded replay, set the feature topic and Redis namespace
explicitly:

```bash
.venv/bin/python -m featureforge_stream.materialize \
  --bootstrap-servers 127.0.0.1:9092 \
  --redis-url redis://127.0.0.1:6379/0 \
  --topic features.replay.my-run.user-play-count-1h.v1 \
  --group-id materialize-my-run-v1 \
  --key-prefix featureforge-stream:replay:my-run:user_play_count_1h \
  --bounded
```

Create and populate the feature topic before running this command.

Use a new group and Redis prefix for an independent verification run. To resume
an interrupted materialization, keep its original group, topic, and Redis prefix.

The materializer captures the topic end offset at startup. It resumes from the
group's committed offset when available, otherwise from the earliest available
offset. It rejects an empty readable range. If the group's committed position
already reaches the captured end offset of a non-empty range, it returns without
processing additional records.

Bounded mode does not implement a progress timeout. If polling stops yielding
records before the captured boundary is reached, the process can keep waiting.

## Historical replay

FeatureForge Parquet events can be adapted and replayed into a dedicated Kafka topic.

Preview without publishing:

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

The adapter refuses to publish into the normal input topic. Historical replay must use a dedicated topic:

```text
interaction-events.replay.<run-name>.v1
```

### Historical replay topic policy

Create replay topics explicitly before publishing. Isolate input, validated,
rejected, feature, and late-event topics, and configure each job to use them.

For this local historical replay workflow, create all replay topics with:

```text
cleanup.policy=delete
retention.ms=604800000
message.timestamp.type=LogAppendTime
```

Example topic creation:

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

`LogAppendTime` uses broker append time for the Kafka record timestamp. It does
not rewrite JSON fields such as `event_time`, `ingested_at`, or
`observation_time`. The current feature job assigns event time from the JSON
`event_time` field, not from the Kafka record timestamp.

Historical Kafka record timestamps combined with `CreateTime` and time-based
retention can cause newly replayed output to be deleted as already expired.
This occurred during local verification and was confirmed in broker logs.

Check both earliest and latest offsets before consuming replay output. A large
end offset alone does not prove that records remain readable. Changing a topic
policy does not restore records already deleted.

Run the bounded stages in order: publish input, finish validation, finish
feature computation, then materialize. Each stage captures its input boundary
when it starts.

See [replay architecture](ARCHITECTURE.md#replay-mode) for execution limits.

## Project structure

```text
featureforge-stream/
├── jobs/
│   ├── user_play_count_1h.py
│   └── validate_and_route_events.py
├── src/
│   └── featureforge_stream/
│       ├── contracts/
│       │   ├── events.py
│       │   ├── feature_time.py
│       │   └── routing.py
│       ├── reference/
│       │   ├── admission.py
│       │   └── user_play_count.py
│       ├── replay.py
│       ├── materialize.py
│       └── online_store.py
├── tests/
├── fixtures/
├── docker/
│   └── flink/
│       └── Dockerfile
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── README.md
└── ARCHITECTURE.md
```

## Technology choices

| Technology | Role |
|---|---|
| Python | Application and feature logic |
| Pydantic | Strict event and feature contracts |
| Apache Kafka | Durable event transport |
| Apache Flink | Stateful event-time stream processing |
| Redis | Online feature storage |
| Docker Compose | Local infrastructure |
| pytest | Unit and integration testing |
| Ruff | Linting and formatting |

## Design boundaries

This project intentionally does **not** claim:

- Exactly-once end-to-end processing
- Unlimited replay protection
- Automatic recovery from every failure mode
- Multi-partition Redis materialization
- General-purpose feature retractions
- Historical availability at every past observation time

Instead, it provides precise, tested guarantees:

- Event-time feature computation
- Logical duplicate suppression
- Explicit late-event correction
- Explicit too-late routing
- Guarded online materialization
- Bounded replay behavior
- Reference parity for the implemented feature

## Roadmap alignment

FeatureForge Stream is the streaming-infrastructure layer of a larger FeatureForge system. It connects the batch/source platform to future ML workflows by producing reliable, versioned, online-ready features.

The project focuses on skills required for a Data & Feature Infrastructure or ML Platform Engineer role:

- Feature contracts
- Event-time correctness
- Stream processing
- Online serving
- Data quality
- Testing
- Operational boundaries
- Production-oriented engineering judgment

### Verify replay feature output against Redis

After replay processing and Redis materialization have finished, compare the
retained Kafka feature output with its isolated Redis namespace:

```bash
.venv/bin/python scripts/verify_kafka_redis.py \
  --topic "<replay-feature-topic>" \
  --key-prefix "<replay-redis-key-prefix>" \
  --bootstrap-servers 127.0.0.1:9092 \
  --redis-url redis://127.0.0.1:6379/0 \
  --timeout-seconds 180
```

Replace both placeholders with the topic and Redis prefix from the same replay
run. Stop writers to that topic and namespace before verification.

The checker selects the latest observation window per user, then the highest
play count within that window. It compares the complete feature values and
reports missing values, differing values, and additional Redis namespace keys.

The current implementation requires:
- exactly one Kafka partition;
- a non-empty retained range starting at offset zero;
- contiguous record offsets and no tombstones;
- unchanged Kafka watermarks during the Kafka read;
- completed materialization and no concurrent writers.

The checker reads feature data without writing Redis values or committing Kafka
offsets. It scans the selected Redis database to identify additional namespace
keys; this scan is not an atomic snapshot.

A successful check prints a JSON report followed by:
`PASS: latest Kafka feature values match Redis`.

A mismatch or unsupported input causes a non-zero exit status. This verifies
materialization consistency for a quiescent replay run; it does not establish
batch-stream parity, replay completeness relative to the original source, or
checkpoint recovery.

### Export and verify a replay snapshot

After replay processing and Redis materialization have finished, export the
retained feature windows and compare the latest persisted values with Redis:

```bash
.venv/bin/python scripts/export_replay_snapshot.py \
  --topic "<replay-feature-topic>" \
  --key-prefix "<replay-redis-key-prefix>" \
  --output-dir "output/stream_snapshots/<unique-export-id>" \
  --timeout-seconds 180
```

Replace all placeholders. Use the topic and Redis namespace from the same
completed replay, stop writers, and choose an output directory that does not
already exist.

The export creates windows.parquet and verification.json. The report records
Kafka offset boundaries, row counts, the Parquet SHA-256, and comparison results.
A successful run prints:
`PASS: persisted snapshot latest values match Redis`.

This is a run-scoped streaming output artifact, not Project 1's canonical
offline store or an independent batch-stream parity proof.

See [Stream Snapshot V1](docs/contracts/user-play-count-1h-snapshot-v1.md)
for schema, time semantics, prerequisites, and failure boundaries.

## License

This project is licensed under the [MIT License](LICENSE).
