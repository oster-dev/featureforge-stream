"""Arrival-order admission policy for event-time stream semantics."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from featureforge_stream.contracts.events import InteractionEventV1


class Admission(StrEnum):
    ON_TIME = "on_time"
    ALLOWED_LATE = "allowed_late"
    TOO_LATE = "too_late"


@dataclass(frozen=True)
class TimePolicy:
    watermark_delay: timedelta
    allowed_lateness: timedelta

    def __post_init__(self) -> None:
        if self.watermark_delay < timedelta(0):
            raise ValueError("watermark_delay must not be negative")
        if self.allowed_lateness < timedelta(0):
            raise ValueError("allowed_lateness must not be negative")


@dataclass(frozen=True)
class AdmissionDecision:
    event: InteractionEventV1
    admission: Admission
    boundary: datetime | None


def classify_event_time(
    event_time: datetime,
    max_event_time_seen: datetime | None,
    policy: TimePolicy,
) -> Admission:
    """Classify one event time against the maximum event time seen before it."""
    if max_event_time_seen is None:
        return Admission.ON_TIME

    boundary = max_event_time_seen - policy.watermark_delay
    if event_time >= boundary:
        return Admission.ON_TIME
    if event_time >= boundary - policy.allowed_lateness:
        return Admission.ALLOWED_LATE
    return Admission.TOO_LATE


def admit_in_arrival_order(
    events: Iterable[InteractionEventV1],
    *,
    policy: TimePolicy,
) -> tuple[AdmissionDecision, ...]:
    """Classify events in the order supplied.

    Each event is checked against the maximum event time seen before it.
    Input order is treated as arrival order, so reordering can change results.
    """
    max_seen: datetime | None = None
    decisions: list[AdmissionDecision] = []

    for event in events:
        if not isinstance(event, InteractionEventV1):
            raise TypeError("events must contain InteractionEventV1 instances")

        boundary = None if max_seen is None else max_seen - policy.watermark_delay
        decisions.append(
            AdmissionDecision(
                event=event,
                admission=classify_event_time(event.event_time, max_seen, policy),
                boundary=boundary,
            )
        )
        if max_seen is None or event.event_time > max_seen:
            max_seen = event.event_time

    return tuple(decisions)
