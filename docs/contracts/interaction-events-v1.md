# Interaction Events V1

Status: implemented executable input contract.

Default input topic: `interaction-events.v1`.

Kafka transport, PyFlink validation, structured rejection routing, and
downstream event-time feature processing are implemented. Schema Registry
integration is not implemented.

## Scope

This contract describes synthetic personalization interactions.

All nine fields are required. Unknown fields are rejected.

| Field | Contract |
|---|---|
| `event_id` | UUID identifying one immutable logical event |
| `event_time` | Timezone-aware event occurrence timestamp |
| `ingested_at` | Timezone-aware original platform input receipt timestamp |
| `user_id` | Strict, non-empty string without whitespace |
| `content_id` | Strict, non-empty string without whitespace |
| `event_type` | `impression`, `play`, `skip`, or `complete` |
| `play_seconds` | Strict, non-negative integer |
| `source` | `web`, `mobile`, or `tv` |
| `schema_version` | `v1` |

The executable model is `InteractionEventV1` in
`featureforge_stream.contracts.events`.

## Example

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

## Time

### Accepted timestamps

Wire producers should emit RFC 3339 UTC timestamps using `Z`.

The Python validator accepts timezone-aware ISO 8601 datetime strings with a
`T` separator and explicit timezone information. It also accepts timezone-aware
Python `datetime` instances through the Python validation interface.

Accepted timestamps are normalized to UTC.

Naive timestamps and numeric epoch inputs are rejected.

The validator is not a complete lexical RFC 3339 conformance checker.
Producers should not rely on every ISO 8601 variation accepted by the Python
implementation being portable to other consumers.

### Event time and ingestion time

`event_time` describes when the interaction occurred.

`ingested_at` describes the original platform input receipt time. It must be
on or after `event_time`.

Replaying an event must preserve both timestamps. A replay must not replace
historical receipt time with the current wall-clock time.

Future transport-attempt timestamps belong in a separate envelope rather than
changing the logical event.

A positive ingestion delay does not by itself determine whether Flink accepts
an event into a feature window. The feature job makes that decision using its
current event-time watermark and the cleanup boundary of each assigned window.

### Precision and window assignment

The Python model represents timestamps at Python `datetime` precision.

The `user_play_count_1h` job uses millisecond window-assignment timestamps.
Its feature interval is right-closed:

```text
(observation_time - 1 hour, observation_time]
```

Flink's native windows are half-open:

```text
[start, end)
```

The job therefore maps an event time before assigning windows:

```text
mapped_timestamp_ms = ceil(event_time_in_microseconds / 1000) - 1
```

The implementation uses integer arithmetic to avoid floating-point timestamp
rounding.

This preserves membership at whole-millisecond window boundaries, including
events immediately before or after a boundary.

The mapped value is an internal window-assignment timestamp. It does not
replace the original `event_time` in the event contract.

## Event Meaning

| Event type | Meaning |
|---|---|
| `impression` | Content was exposed to the user |
| `play` | Playback started |
| `skip` | The interaction was explicitly skipped |
| `complete` | Playback completion was reported |

`play_seconds` is the playback duration reported with this interaction. It is
not automatically an additive session counter.

Rules:

- `impression` requires `play_seconds = 0`.
- Other supported event types permit zero or positive seconds.
- Boolean values, floating-point values, numeric strings, negative values,
  and null are not valid `play_seconds` values.

Session consistency, content-duration validation, and relationships between
separate interactions are outside this single-event model.

For `user_play_count_1h`, each unique `play` contributes one count, regardless
of its `play_seconds` value. Other event types do not increase that count.

## Identity

Repeated delivery of the same logical event must preserve `event_id`.

A producer must not assign the same ID to different logical events.

UUID syntax validation does not establish uniqueness.

### Logical payload

Logical identity includes every contract field except `ingested_at`:

- `event_id`
- `event_time`
- `user_id`
- `content_id`
- `event_type`
- `play_seconds`
- `source`
- `schema_version`

`ingested_at` is excluded from logical payload comparisons because receipt
metadata does not change the business meaning of the interaction.

Producers and replay tools must nevertheless preserve the original
`ingested_at`. Excluding it from conflict detection is not permission to
rewrite receipt history.

### Single-event validation

`InteractionEventV1` validates one event at a time.

It does not independently:

- Deduplicate a collection of events.
- Detect conflicting payloads across records.
- Establish global event-ID uniqueness.

Those responsibilities belong to stateful processing and reference logic.

### Reference identity checking

`count_user_plays_1h` checks identities across its entire supplied input before
applying user, event-type, or time-window filters.

Repeated identical logical events count once.

A repeated ID with a conflicting logical payload raises
`EventIdentityConflictError`, even if the conflicting event is outside the
requested user or window.

### Streaming identity checking

The feature job keys deduplication state by `event_id`.

Within retained state:

- A first delivery is emitted downstream.
- An identical logical delivery is suppressed.
- A conflicting logical delivery raises `EventIdentityConflictError`.

The streaming deduplication state has a 24-hour processing-time TTL.

Suppressed duplicates do not refresh that retention period.

This is bounded duplicate protection, not unlimited historical replay safety.
After state expires, the job no longer has the previous payload available for
duplicate or conflict detection.

## Validation and Routing

The validation job consumes the configured input topic and calls
`route_event_json`.

The default routes are:

| Outcome | Default destination |
|---|---|
| Valid, normalized event | `interaction-events.validated.v1` |
| Input validation failure | `interaction-events.rejected.v1` |

Each routing result contains either valid JSON or rejection JSON, not both.

### Rejection structure

A rejection contains:

| Field | Meaning |
|---|---|
| `error_schema_version` | Rejection-envelope version, currently `v1` |
| `reason_code` | Machine-readable failure category |
| `raw_payload` | Original input payload |
| `errors` | Structured validation errors |

Reason codes:

| Reason code | Meaning |
|---|---|
| `INVALID_JSON` | The input is not valid JSON |
| `EVENT_CONTRACT_VIOLATION` | The input parses as JSON but violates the event contract |

Structured errors exclude Pydantic documentation URLs, validation context, and
per-error input values. The rejection still contains the complete original
payload in `raw_payload`.

A validation failure does not block subsequent valid records.

Unexpected programming or runtime errors are not converted into validation
rejections. They remain visible to the caller or processing runtime.

## Downstream Event-Time Processing

The current `user_play_count_1h` job uses:

| Setting | Value |
|---|---|
| Window length | 1 hour |
| Window slide | 1 minute |
| Out-of-orderness allowance | 10 minutes |
| Allowed lateness | 30 minutes |

Allowed lateness applies to assigned windows, not to a universal
`event_time` age cutoff.

An arriving event may still update some assigned windows after other assigned
windows have expired.

If every assigned window is too late, the window operator routes the event to:

```text
interaction-events.user-play-count-1h.late.v1
```

The late-event envelope uses:

```text
reason_code = TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS
```

This route is distinct from contract rejection:

- Rejected events fail input validation.
- Too-late events can be contract-valid but no longer eligible for any
  assigned feature window.

The arrival-order helper in `reference.admission` is a separate simplified
classification model. Its event-level bands must not be treated as an exact
simulation of Flink's sliding-window admission or watermark scheduling.

## Boundary with FeatureForge Batch

This is a stream contract, not a drop-in replacement for the Project 1 event
model or its completed batch feature tables.

The existing adapter consumes FeatureForge source-event Parquet output.
It does not import Project 1 application internals.

Required source columns are:

```text
event_id
user_id
content_id
event_type
event_time
ingested_at
device_type
watch_seconds
```

### Supported event types

The adapter currently accepts:

- `play`
- `impression`

Other source event types are skipped and reported. This is an adapter support
boundary, not a restriction of the stream contract itself.

### Field mapping

| Source field | Stream field or behavior |
|---|---|
| `event_id` | Deterministic UUID derived from dataset identity and source ID |
| `user_id` | `user_id` |
| `content_id` | `content_id` |
| `event_type` | Preserved for supported types |
| `event_time` | Preserved and validated |
| `ingested_at` | Preserved and validated |
| `watch_seconds` | `play_seconds` |
| `device_type` | Mapped to `source` |
| No source equivalent | `schema_version = v1` |

Device mappings:

| Source device | Stream source |
|---|---|
| `web` | `web` |
| `android` | `mobile` |
| `ios` | `mobile` |
| `mobile` | `mobile` |
| `tv` | `tv` |

Unsupported device values on supported event rows fail adaptation.

Supported rows must satisfy the stream contract. The adapter does not silently
repair invalid timestamps, identifiers, durations, or event semantics.

### Dataset identity and replay order

A stable `dataset_id` and source event ID produce a stable derived UUID.

Changing `dataset_id` changes the derived streaming identity. Dataset identity
must therefore remain consistent when replaying the same logical dataset.

The adapter:

- Preserves duplicate deliveries.
- Detects conflicting adapted logical payloads for a derived event ID.
- Sorts deliveries by `ingested_at`.
- Preserves original file order when ingestion timestamps are equal.
- Publishes to partition zero.
- Performs accelerated replay without wall-clock pacing.

Publishing requires a dedicated topic whose name begins with:

```text
interaction-events.replay.
```

The intended naming convention is:

```text
interaction-events.replay.<run-name>.v1
```

Downstream jobs must be explicitly configured to consume and produce the
corresponding replay topics. Publishing to a replay topic does not
automatically connect that topic to jobs using their default input settings.

## Evolution

Unknown fields are rejected by the current V1 consumer.

This is a strict consumer policy, not automatic forward compatibility.

Adding even an optional field requires a coordinated consumer update before
producers emit it.

A change to any of the following requires an explicit compatibility decision:

- Required fields or accepted values.
- Entity-identifier meaning.
- Event-type meaning.
- Timestamp representation or precision.
- Logical identity rules.
- Adapter mappings.
- Feature interpretation.

Breaking changes require a version and migration decision.

The `schema_version` field identifies the payload version. It does not replace
compatibility tests or migration procedures.

## Verification Coverage

The supplied unit tests cover:

- Required fields.
- Supported event types and sources.
- UUID input validation.
- Timestamp validation and UTC normalization.
- Ingestion ordering.
- Strict duration validation.
- Entity-identifier validation.
- Unknown-field rejection.
- Normal assignment protection.
- JSON round trips.
- Valid/rejected routing.
- Reference duplicate and conflict behavior.
- Right-closed feature boundaries.

The supplied integration tests cover scenarios for:

- Kafka validation and rejection routing.
- Flink values against the golden fixture.
- Late corrections to an emitted window.
- Duplicate suppression.
- Routing events too late for all assigned windows.
- Guarded Redis storage and a real concurrent-write conflict.

Test definitions describe coverage; passing results must be established by
executing them in the relevant environment.

## Guarantee Boundaries

This input contract does not by itself establish:

- Exactly-once end-to-end delivery.
- Unlimited duplicate protection.
- Checkpoint recovery correctness.
- Complete cross-project feature parity.
- Historical online availability.
- Automatic freshness or zero-value expiry.
- Feast-compatible Redis storage.
- Production readiness.

The current downstream feature store retains the latest written observation
window per user. It does not generate new observations or expire values merely
because wall-clock time advances.

Input validation, event-time computation, transport delivery, replay isolation,
and online serving are separate responsibilities. Each requires its own
implementation and verification evidence.