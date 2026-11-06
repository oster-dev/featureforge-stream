"""Map precise event times to right-closed millisecond window membership."""

from datetime import UTC, datetime

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def right_closed_timestamp_ms(event_time: datetime) -> int:
    """Map event_time for windows whose boundaries are whole milliseconds.

    Flink uses [start, end), while the feature contract uses (start, end].

    ceil(event_time_in_microseconds / 1000) - 1 preserves membership:
    start < event_time <= end
    iff
    start_ms <= mapped_timestamp < end_ms.

    This is a window-assignment timestamp, not the original event timestamp.
    """
    if not isinstance(event_time, datetime):
        raise TypeError("event_time must be a datetime")
    if event_time.utcoffset() is None:
        raise ValueError("event_time must be timezone-aware")

    delta = event_time.astimezone(UTC) - EPOCH
    microseconds = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds

    return (microseconds + 999) // 1000 - 1
