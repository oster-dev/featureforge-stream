# FeatureForge Stream

A production-inspired local reference implementation for near-real-time
feature computation.

The planned platform extends batch feature engineering into stateful,
event-time streaming, with explicit contracts for duplicates, late events,
replay and online/offline consistency.

## Current status

Initial Python development environment is ready.

Implemented and verified:
- Installable Python package using a src layout.
- Python 3.11 development environment.
- Package import and installed-version tests.
- Ruff linting and formatting checks.
- A single command for local quality checks: `make check`.

Kafka, Flink, Redis, streaming features and parity checks are not
implemented yet.

## Local development

Prerequisite: Python 3.11 available as `python3.11`.

Run these commands from the repository root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
make check
```

## Quality checks

| Command | Purpose |
|---|---|
| `make check` | Run lint, formatting checks and tests |
| `make lint` | Run Ruff lint checks |
| `make format-check` | Check formatting without modifying files |
| `make format` | Format Python files |
| `make test` | Run the test suite |

## Scope

This project is a local reference implementation, not a claim of
production deployment or Netflix-scale operation.

Correctness guarantees will be documented alongside their tests as
implementation progresses.