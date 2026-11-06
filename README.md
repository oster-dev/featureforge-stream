# FeatureForge Stream

A production-inspired local reference implementation for near-real-time
feature computation.

FeatureForge Stream extends the batch-first foundation of
[FeatureForge](https://github.com/oster-dev/featureforge) toward stateful,
event-time streaming, with explicit contracts for event identity,
time windows, replay, and offline/online consistency.

The current implementation runs two PyFlink jobs on a local Kafka/Flink
stack and computes the first streaming feature: `user_play_count_1h`.

## Current Status

Implemented and locally verified:

- Installable Python package with a `src` layout.
- Validated, immutable interaction-event model.
- Deterministic feature reference and golden fixture.
- Arrival-order admission reference and boundary tests.
- Docker Compose stack with Kafka and Flink.
- Python workers executing application code on the TaskManager.
- Validation routing into separate valid and rejected Kafka topics.
- Stateful deduplication by event ID with bounded retention.
- Event-time, minute-aligned rolling-hour feature computation.
- Flink checkpoint creation.
- Kafka feature output and a separate late-event output.
- End-to-end comparison of Flink feature values with the Python reference.

Latest verified local results:

```text
Ruff lint: passed
Ruff format: passed
Default test run: 120 passed, 2 deselected
Integration test run: 2 passed
```

The integration tests require the running local stack and both submitted jobs.
They are excluded from the default test run.

## Architecture

```text
Interaction-event producer / integration fixture
                    ↓
         interaction-events.v1
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
```

### Local Components

| Component | Current implementation |
|---|---|
| Kafka | Apache Kafka 3.9.1, single local broker/controller |
| Flink | Apache Flink 1.20.3 |
| JobManager | One container |
| TaskManager | One container with two task slots |
| Jobs | Validation job and feature job, each at parallelism 1 |
| Application runtime | Python 3.11 in the Flink image |
| Transport and output | Kafka topics |
| Local Kafka persistence | Docker named volume |

This is a local reference setup, not a highly available deployment.

### Jobs

| Job | Entry point |
|---|---|
| Validation and error routing | `jobs/validate_and_route_events.py` |
| Rolling-hour play count | `jobs/user_play_count_1h.py` |

### Kafka Topics

| Topic | Purpose |
|---|---|
| `interaction-events.v1` | Raw interaction-event JSON |
| `interaction-events.validated.v1` | Validated and normalized events |
| `interaction-events.rejected.v1` | Input-validation failures |
| `features.user-play-count-1h.v1` | Feature updates |
| `interaction-events.user-play-count-1h.late.v1` | Events too late for all assigned feature windows |

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

- an error schema version;
- a reason code;
- the original payload;
- structured validation errors.

The validation integration test uses unique records for each run and checks
that a malformed record does not prevent a following valid record from
reaching the validated topic.

Contract:
[Interaction Events V1](docs/contracts/interaction-events-v1.md)

## Feature: User Play Count 1h

### Reference Definition

`count_user_plays_1h` counts unique play events for an explicitly requested
user within:

```text
(observation_time - 1 hour, observation_time]
```

The lower boundary is excluded; the upper boundary is included.

The reference:

- counts repeated deliveries of the same logical event once;
- rejects conflicting logical payloads sharing an event ID;
- validates identities before applying query filters;
- uses explicit, timezone-aware observation timestamps;
- returns zero when no eligible event exists;
- produces the same count regardless of input ordering;
- preserves supplied event objects.

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

The streaming observation times are minute-aligned. The reference still
supports arbitrary timezone-aware observation timestamps.

### Event Identity and Deduplication

Logical event identity is defined by `event_id`.

The logical payload includes:

- `event_time`;
- `user_id`;
- `content_id`;
- `event_type`;
- `play_seconds`;
- `source`;
- `schema_version`.

`ingested_at` is receipt metadata and is excluded from logical-payload
comparison.

Within the retained deduplication state:

- repeated delivery of the same logical event is suppressed;
- a conflicting logical payload raises `EventIdentityConflictError`;
- repeated reads do not extend the deduplication retention period.

Deduplication happens before user-level window aggregation.

The 24-hour processing-time TTL bounds identity retention. It is not an
unlimited replay guarantee. After expiration, an event ID can be treated as
new again.

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

### Late Events and Event-Time Progress

Allowed lateness is evaluated using Flink's window lifecycle.

An event can still update some assigned sliding windows while other assigned
windows are already closed.

An event that is too late for all assigned windows is routed to the late
topic with its original payload and the reason code:

```text
TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS
```

The separate arrival-order admission reference is a semantic reference,
not a claim that its per-event classification is identical to Flink's
window-based lateness handling.

Event-time windows depend on watermark progress. Waiting in wall-clock time
does not by itself close the last windows of a quiet, unbounded Kafka stream.

The feature integration test therefore supplies an explicit event-time
progress event.

### Empty Windows

The current job does not generate periodic output for completely empty
windows or unknown users.

It also does not yet emit automatic zero updates after all of a user's
events expire.

The reference's ability to return zero for an explicit query is therefore
not yet a complete online-serving contract.

## Golden Fixture

The version-controlled fixture is:

```text
fixtures/user_play_count_1h_v1.jsonl
```

It contains eight delivery records representing seven distinct logical
events, including a repeated event, window-boundary events, a non-play
event, and a second user.

At observation time `2026-01-01T12:00:00Z`:

| Requested user | Expected reference count |
|---|---|
| `user_000001` | 3 |
| `user_000002` | 1 |
| `user_unknown` | 0 |

The streaming integration test:

- assigns unique IDs for each test run;
- shifts fixture timestamps to a current minute-aligned observation time;
- preserves relative event times and duplicate identity;
- sends events chronologically;
- advances event time beyond the observation window;
- compares the emitted counts with `count_user_plays_1h`.

The current fixture-based integration test proves ordered-input counting,
duplicate suppression, entity separation, and window-boundary behavior.
It does not yet prove late-event correction or recovery after failure.

## Local Development

Prerequisites:

- Python 3.11.
- Docker with Docker Compose.
- Sufficient Docker memory for Kafka, the JobManager, and the TaskManager.
- Available local ports `9092` and `8081`.

Run from the repository root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
make check
```

PyFlink executes inside the custom Flink image. The base development
environment does not require a local PyFlink installation.

## Run the Streaming Stack

### 1. Build and Start

```bash
docker compose build jobmanager

docker compose up -d \
  --wait \
  --wait-timeout 180 \
  kafka jobmanager taskmanager
```

Check Flink registration:

```bash
curl --fail --silent --show-error \
  http://localhost:8081/overview
```

Before submitting jobs, expect one TaskManager and two available slots.

### 2. Create Topics

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

### 3. Submit Both Jobs

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

Check job states:

```bash
curl --fail --silent --show-error \
  http://localhost:8081/jobs/overview
```

Both jobs should report `RUNNING`.

Flink Web UI:

```text
http://localhost:8081
```

### 4. Run Integration Tests

With both jobs running:

```bash
python -m pytest -q -m integration tests/integration
```

Latest verified result:

```text
2 passed
```

These tests write synthetic records into the local Kafka topics.

They use unique identifiers to distinguish their records from historical
topic contents. The feature test also advances event time. Replaying the
unmodified January fixture afterward can therefore exercise late-event
handling rather than the original ordered-input scenario.

### 5. Inspect Feature Output

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic features.user-play-count-1h.v1 \
  --from-beginning \
  --timeout-ms 5000
```

The console consumer can print a timeout message after the configured idle
period. Inspect the returned records separately from that termination message.

### 6. Stop the Local Stack

```bash
docker compose stop
```

This retains the containers and Kafka volume.

After recreating the Flink cluster, submit the jobs again. Automatic
restoration across a complete local cluster restart has not yet been
demonstrated.

Do not remove the Kafka volume unless deliberately resetting local data.

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
| `python -m pytest -q -m integration tests/integration` | Run both integration tests against the running stack |

The `integration` marker is registered in `pyproject.toml`.
Default pytest options exclude it; an explicit `-m integration` selects it.

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
| Kafka → Flink feature values compared with the reference | `tests/integration/test_user_play_count_job.py` |

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
- Both sources start from the earliest retained offsets on a fresh submission
  without restored state.
- Fresh submissions can reproduce historical output.
- Checkpoint creation is not a demonstrated recovery drill.
- Durable, externally addressable checkpoint storage is not yet configured.
- End-to-end exactly-once delivery is not claimed.
- Replay safety beyond the deduplication retention period is not claimed.

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
│       └── reference/
│           ├── admission.py
│           └── user_play_count.py
├── tests/
│   ├── integration/
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

This is a focused view of the implemented streaming path, not an exhaustive
listing of every repository directory.

## Remaining Work

The first feature path works, but the complete platform is not finished.

Remaining work includes:

- Integration tests for late corrections, too-late routing, and duplicate
  delivery after an initial feature result.
- Tested zero-value and expiry behavior for online serving.
- Durable checkpoint configuration and controlled restart/replay drills.
- Idempotent Redis updates with stale-write protection.
- Offline Parquet snapshots.
- Spark/Flink and offline/online parity.
- Additional documented features and a leakage-safe label contract.
- Schema compatibility management.
- Operational metrics, dashboard, and concise runbooks.
- A constrained feature-specification and practitioner workflow.
- Updated architecture documentation, demo, and release packaging.

The implementation favors Flink and Kafka's built-in capabilities.
Additional abstractions or services are introduced only when required by
a concrete correctness, usability, or operational need.

## Relationship to FeatureForge

FeatureForge provides the batch-first platform foundation: historical feature
computation, canonical offline storage, quality gates, Feast retrieval, and
online materialization.

FeatureForge Stream is developed separately and introduces its own versioned
interaction-event contract and rolling-hour feature definition.

The reference feature in this repository is independently defined.
It does not modify existing FeatureForge feature views or data partitions.

## Scope and Non-Goals

This repository is a production-inspired local reference implementation,
not a production deployment or a claim of Netflix-scale operation.

It does not attempt to build:

- a custom streaming engine;
- a general-purpose feature compiler;
- a replacement for Flink, Kafka, or Feast;
- an unrelated recommendation or vector-search system;
- a Kubernetes platform.

AWS deployment considerations belong in an explicit, cost-aware production
mapping. Running the local stack does not establish AWS deployment,
high availability, or production readiness.

Project claims are limited to implemented behavior and executable evidence.