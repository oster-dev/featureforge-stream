"""Event-time reference for user_skip_rate_24h."""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from uuid import UUID

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.reference.user_play_count import EventIdentityConflictError


def calculate_user_skip_rate_24h(
    events: Iterable[InteractionEventV1],
    *,
    user_id: str,
    observation_time: datetime,
) -> float:
    """Calculate skips / (plays + skips) over the prior right-closed 24h.

    Validate identities across the entire input before applying filters.
    Return 0.0 when no eligible plays or skips exist.
    This reconstructs event time, not historical online availability.
    """
    if not isinstance(user_id, str):
        raise TypeError("user_id must be a string")
    if not user_id or any(character.isspace() for character in user_id):
        raise ValueError("user_id must be non-empty and contain no whitespace")

    if not isinstance(observation_time, datetime):
        raise TypeError("observation_time must be a datetime")
    if observation_time.utcoffset() is None:
        raise ValueError("observation_time must be timezone-aware")

    cutoff = observation_time.astimezone(UTC)
    window_start = cutoff - timedelta(hours=24)
    unique_events: dict[UUID, InteractionEventV1] = {}

    for event in events:
        if not isinstance(event, InteractionEventV1):
            raise TypeError("events must contain InteractionEventV1 instances")

        previous = unique_events.get(event.event_id)
        if previous is not None:
            if previous.model_dump(exclude={"ingested_at"}) != event.model_dump(
                exclude={"ingested_at"}
            ):
                raise EventIdentityConflictError(
                    f"Conflicting logical payload for event_id={event.event_id}"
                )
        else:
            unique_events[event.event_id] = event

    plays = 0
    skips = 0

    for event in unique_events.values():
        if event.user_id != user_id:
            continue
        if not window_start < event.event_time <= cutoff:
            continue
        if event.event_type == "play":
            plays += 1
        elif event.event_type == "skip":
            skips += 1

    denominator = plays + skips
    return skips / denominator if denominator else 0.0
