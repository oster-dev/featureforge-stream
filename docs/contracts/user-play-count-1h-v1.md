# User Play Count 1h V1

A deterministic event-time reference for counting unique user play events
within a rolling one-hour window.

## Implementation

Module:

```text
src/featureforge_stream/reference/user_play_count.py
```

Interface:

```python
count_user_plays_1h(
    events,
    *,
    user_id,
    observation_time,
) -> int
```

The function accepts validated InteractionEventV1 instances, an explicit
user ID, and a timezone-aware observation timestamp.

It returns a non-negative integer.

## Feature Definition

Count unique play events for the requested user within:

```text
(observation_time - 1 hour, observation_time]
```

- The lower boundary is excluded.
- The upper boundary is included.
- Only matching user IDs contribute.
- Only play events contribute.
- An empty eligible event set returns zero.

Observation time is normalized to UTC.
Window membership is determined by event_time.

## Event Identity

Logical event identity is defined by event_id.

Repeated records with the same event_id and logical payload count once.

The logical payload includes:

- event_time;
- user_id;
- content_id;
- event_type;
- play_seconds;
- source;
- schema_version.

ingested_at is receipt metadata and is excluded from logical-payload
comparison.

Records sharing an event_id but differing in logical payload raise
EventIdentityConflictError.

Identity validation applies to the complete supplied input before filtering
by user, event type, or time window.

## Time Semantics

The reference reconstructs feature values from the event times of the
supplied records.

An event can contribute even when its ingested_at timestamp is later than
observation_time.

Equivalent observation timestamps expressed with different UTC offsets
produce the same count.

### Window Boundaries

For observation_time equal to 2026-01-01T12:00:00Z:

| Event time | Included |
|---|---|
| 2026-01-01T10:59:59.999999Z | No |
| 2026-01-01T11:00:00Z | No |
| 2026-01-01T11:00:00.000001Z | Yes |
| 2026-01-01T12:00:00Z | Yes |
| 2026-01-01T12:00:00.000001Z | No |

### Window Expiry

A play at 12:00:00Z:

- contributes at observation time 12:59:00Z;
- no longer contributes at observation time 13:00:00Z.

The caller supplies the observation time for each calculation.

## Deterministic Behavior

For valid, conflict-free input:

- input ordering does not change the count;
- repeated delivery does not increase the count;
- supplied event objects remain unchanged.

The function consumes the supplied iterable and retains unique events in
memory during the calculation.

## Golden Fixture

Location:

```text
fixtures/user_play_count_1h_v1.jsonl
```

The fixture contains eight delivery records representing seven distinct
logical events.

Observation time:

```text
2026-01-01T12:00:00Z
```

| Event | Event time | User | Type | Contribution for user_000001 |
|---|---|---|---|---|
| A | 11:00:00Z | user_000001 | play | 0 |
| B | 11:00:01Z | user_000001 | play | 1 |
| C | 11:30:00Z | user_000001 | play | 1 |
| C repeated | 11:30:00Z | user_000001 | play | 0 additional |
| D | 12:00:00Z | user_000001 | play | 1 |
| E | 12:00:01Z | user_000001 | play | 0 |
| F | 11:40:00Z | user_000001 | skip | 0 |
| G | 11:45:00Z | user_000002 | play | 0 |

Expected results:

| Requested user | Count |
|---|---|
| user_000001 | 3 |
| user_000002 | 1 |
| user_unknown | 0 |

## Input Validation and Errors

| Condition | Exception |
|---|---|
| user_id is not a string | TypeError |
| user_id is empty or contains whitespace | ValueError |
| observation_time is not a datetime | TypeError |
| observation_time is timezone-naive | ValueError |
| An input item is not an InteractionEventV1 instance | TypeError |
| One event_id has conflicting logical payloads | EventIdentityConflictError |

User IDs are checked syntactically. The calculation does not consult a user
registry, so zero is returned for any valid user ID without eligible events.

## Tests

Location:

```text
tests/unit/test_user_play_count.py
```

The 29 feature-reference test cases cover:

- golden-fixture counts;
- window boundaries at microsecond precision;
- exclusion of non-play events;
- empty input;
- repeated delivery with different receipt timestamps;
- identity conflicts outside the query scope;
- conflicting content, event type, playback duration, source, and event time;
- input-order independence;
- preservation of input event data;
- equivalent observation-time offsets;
- events received after observation time;
- naive observation-time rejection;
- invalid user-ID rejection;
- expiry as observation time advances.

Run the feature tests:

```bash
python -m pytest tests/unit/test_user_play_count.py -q
```

Run the local quality checks:

```bash
make check
```