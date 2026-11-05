"""Event-time reference for user_play_count_1h."""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from uuid import UUID

from featureforge_stream.contracts.events import InteractionEventV1


class EventIdentityConflictError(ValueError):
    """One event ID refers to different logical payloads."""


def count_user_plays_1h(
    events: Iterable[InteractionEventV1],
    *,
    user_id: str,
    observation_time: datetime,
) -> int:
    """Count unique plays in (observation_time - 1 hour, observation_time].

    Validate identities across the entire input before applying filters.
    Receipt time is not part of logical identity or window membership.
    This computes event-time reconstruction, not historical availability.
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
    window_start = cutoff - timedelta(hours=1)
    unique_events: dict[UUID, InteractionEventV1] = {}

    for event in events:
        if not isinstance(event, InteractionEventV1):
            raise TypeError("events must contain InteractionEventV1 instances")

        previous = unique_events.get(event.event_id)
        if previous is not None:
            previous_payload = previous.model_dump(exclude={"ingested_at"})
            current_payload = event.model_dump(exclude={"ingested_at"})
            if previous_payload != current_payload:
                raise EventIdentityConflictError(
                    f"Conflicting logical payload for event_id={event.event_id}"
                )
        else:
            unique_events[event.event_id] = event

    return sum(
        1
        for event in unique_events.values()
        if event.user_id == user_id
        and event.event_type == "play"
        and window_start < event.event_time <= cutoff
    )
