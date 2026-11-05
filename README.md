# FeatureForge Stream

A production-inspired local reference implementation for near-real-time
feature computation.

FeatureForge Stream extends the batch-first foundation of
[FeatureForge](https://github.com/oster-dev/featureforge) toward stateful,
event-time streaming, with explicit contracts for event identity,
time windows, replay, and offline/online consistency.

## Current Status

The Python development foundation, validated interaction-event contract,
and first event-time feature reference are implemented.

The current executable path is:

```text
Interaction-event JSON
        ↓
InteractionEventV1 validation
        ↓
Logical event-identity validation
        ↓
Duplicate-safe event-time reference calculation
        ↓
user_play_count_1h
```

### Development Foundation

- Installable Python package using a `src` layout.
- Python 3.11 local development environment.
- Package import and installed-version tests.
- Ruff linting and formatting.
- Local quality checks through `make check`.

### Interaction Event Contract

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

Contract:
[Interaction Events V1](docs/contracts/interaction-events-v1.md)

### User Play Count Reference

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
An event can contribute even when it was received after the observation
timestamp.

Contract:
[User Play Count 1h V1](docs/contracts/user-play-count-1h-v1.md)

## Golden Fixture

The version-controlled fixture is:

```text
fixtures/user_play_count_1h_v1.jsonl
```

It contains eight delivery records representing seven distinct logical
events, including a repeated event, window-boundary events, a non-play
event, and a second user.

At observation time `2026-01-01T12:00:00Z`:

| Requested user | Expected play count |
|---|---|
| `user_000001` | 3 |
| `user_000002` | 1 |
| `user_unknown` | 0 |

## Local Development

Prerequisite: Python 3.11 available as `python3.11`.

Run from the repository root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
make check
```

## Usage

Calculate the reference feature from the golden fixture:

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
| `make check` | Run lint, formatting checks, and tests |
| `make lint` | Run Ruff lint checks |
| `make format-check` | Check formatting without modifying files |
| `make format` | Format Python files |
| `make test` | Run the test suite |

Run individual test modules:

```bash
python -m pytest tests/unit/test_package.py -q
python -m pytest tests/unit/test_events.py -q
python -m pytest tests/unit/test_user_play_count.py -q
```

Latest verified local result:

```text
Ruff lint: passed
Ruff format: passed
pytest: 90 passed
```

### Test Coverage by Concern

| Concern | Executable evidence |
|---|---|
| Package installation metadata and import | `tests/unit/test_package.py` |
| Event structure, types, timestamps, and semantics | `tests/unit/test_events.py` |
| Golden-fixture results and window boundaries | `tests/unit/test_user_play_count.py` |
| Repeated delivery and identity conflicts | `tests/unit/test_user_play_count.py` |
| Ordering, input preservation, and timezone equivalence | `tests/unit/test_user_play_count.py` |
| Window expiry at explicit observation times | `tests/unit/test_user_play_count.py` |

## Implemented Repository Structure

```text
featureforge-stream/
├── docs/
│   └── contracts/
│       ├── interaction-events-v1.md
│       └── user-play-count-1h-v1.md
├── fixtures/
│   └── user_play_count_1h_v1.jsonl
├── src/
│   └── featureforge_stream/
│       ├── __init__.py
│       ├── contracts/
│       │   ├── __init__.py
│       │   └── events.py
│       └── reference/
│           ├── __init__.py
│           └── user_play_count.py
├── tests/
│   └── unit/
│       ├── test_package.py
│       ├── test_events.py
│       └── test_user_play_count.py
├── .gitignore
├── Makefile
├── pyproject.toml
└── README.md
```

## Relationship to FeatureForge

FeatureForge provides the batch-first platform foundation: historical feature
computation, canonical offline storage, quality gates, Feast retrieval, and
online materialization.

FeatureForge Stream is developed separately and introduces its own versioned
interaction-event contract and rolling-hour feature definition.

The reference feature in this repository is independently defined.
It does not modify the existing FeatureForge feature views or data partitions.

## Scope

This repository is a production-inspired local reference implementation,
not a production deployment.

The current feature implementation is an in-memory correctness reference.
Its tested behavior concerns validated input, logical event identity, and
explicit event-time queries.

Project claims are tied to the implementation and executable evidence
described above.