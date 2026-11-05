# Interaction Events V1

Status: initial executable input contract.
Topic name reserved by the design: `interaction-events.v1`.
Kafka transport and schema-registry integration are not implemented yet.

## Scope

This contract describes synthetic personalization interactions.
All nine fields are required.

| Field | Contract |
|---|---|
| event_id | UUID identifying one immutable logical event |
| event_time | Timezone-aware event occurrence timestamp |
| ingested_at | Timezone-aware receipt timestamp at the platform input boundary |
| user_id | Non-empty synthetic identifier without whitespace |
| content_id | Non-empty synthetic identifier without whitespace |
| event_type | impression, play, skip, or complete |
| play_seconds | Strict non-negative integer |
| source | web, mobile, or tv |
| schema_version | v1 |

## Time

Wire producers should emit RFC 3339 UTC timestamps using `Z`.
The initial Python validator also accepts timezone-aware ISO 8601 timestamps
with explicit offsets and normalizes them to UTC.

Naive timestamps and numeric epoch inputs are rejected.
Ingestion time must be on or after event time.

`ingested_at` describes the original platform input receipt.
Replaying an event must preserve this field.
Future transport-attempt timestamps belong in a separate envelope.

A positive ingestion delay does not by itself mean that Flink must reject
an event. Watermark and allowed-lateness policy will define that behavior.

This initial validator is not a complete lexical RFC 3339 conformance checker.
Cross-engine timestamp precision will be specified before Flink integration.

## Event meaning

- impression: content was exposed to the user;
- play: playback started;
- skip: the interaction was explicitly skipped;
- complete: playback completion was reported.

`play_seconds` is the playback duration reported with this interaction.
It is not automatically an additive session counter.
Impressions require zero seconds.
Other event types currently permit zero or positive seconds.

Session consistency and content-duration checks are outside this model.

## Identity

Repeated delivery of the same logical event must preserve `event_id`.
A producer must not assign the same ID to different logical events.

The single-event model does not implement deduplication or detect conflicts
between multiple records. Those guarantees require state and separate tests.

UUID syntax validation does not prove uniqueness.

## Evolution

Unknown fields are rejected by the initial V1 consumer.

This is a deliberately strict consumer policy, not a claim of automatic
forward compatibility. Adding an optional field requires a coordinated
consumer update before producers emit that field.

Breaking changes require a version and migration decision.

## Boundary with FeatureForge Batch

This is a new stream contract, not a drop-in replacement for the Project 1
event model.

Project 1 event types and fields must not be silently renamed or reinterpreted.
Any adapter requires explicit mappings and rejection rules.

## Current proof

Unit tests cover required fields, supported values, identity syntax,
timestamp validation, UTC normalization, ingestion order, numeric strictness,
unknown fields, normal assignment protection, and JSON round trips.

No Kafka, Flink, DLQ, deduplication, replay-safety, or serving guarantee is
claimed by this implementation.