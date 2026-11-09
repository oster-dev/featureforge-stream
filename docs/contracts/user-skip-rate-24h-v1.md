# User Skip Rate 24h V1

## Scope and interface

Deterministic Python event-time reference only.
This document does not claim Flink, Redis, or snapshot support for this feature.

Module: `src/featureforge_stream/reference/user_skip_rate.py`

```python
calculate_user_skip_rate_24h(
    events,
    *,
    user_id,
    observation_time,
) -> float
```

## Definition

For the requested user, count unique events within:

```text
(observation_time - 24 hours, observation_time]
```

Return:

```text
skip_count / (play_count + skip_count)
```

- The lower boundary is excluded; the upper boundary is included.
- A zero denominator returns `0.0`.
- Output is a float in `[0.0, 1.0]`, not a percentage.
- Impression and complete events contribute to neither count.
- Content IDs do not partition this user-level feature.
- This measures event counts, not session-level skip probability.
- Play and skip events are distinct interactions; no session pairing is inferred.

## Identity and validation

Identity is `event_id`. Repeated identical logical events contribute once.
Logical payload includes every event field except `ingested_at`.

Conflicting payloads raise the existing `EventIdentityConflictError`.
Identity validation covers the entire supplied input before filtering,
including other users, excluded event types, and out-of-window events.

Input items must be validated `InteractionEventV1` instances.
User IDs must be non-empty strings containing no whitespace.
Observation time must be a timezone-aware datetime.

Invalid argument types raise `TypeError`; invalid user IDs and naive
observation timestamps raise `ValueError`.

## Time and determinism

Observation time is normalized to UTC. Membership uses `event_time`.
Receipt after observation does not exclude an otherwise eligible event.

This reconstructs event-time values from supplied records.
It does not prove historical online availability or leakage-safe retrieval.
It does not model watermark admission or allowed-lateness policy.

For conflict-free input, ordering and repeated delivery do not change
the result. Input events are not mutated. The iterable is consumed once
and unique events are retained in memory.

Advancing observation time expires events from both numerator and denominator.

## Hand-calculated examples

All listed events belong to the requested user and are inside the window.

| Events | Expected |
|---|---|
| None | 0.0 |
| One play | 0.0 |
| One skip | 1.0 |
| One play, one skip | 0.5 |
| Two plays, one skip | 1/3 |
| One play, two skips | 2/3 |
| Impression and complete only | 0.0 |

## Verification

```bash
python -m pytest tests/unit/test_user_skip_rate.py -q
make check
```
