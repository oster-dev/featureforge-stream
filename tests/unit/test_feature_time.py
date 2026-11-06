"""Window-assignment timestamps preserve the existing feature boundaries."""

from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.contracts.feature_time import right_closed_timestamp_ms
from featureforge_stream.reference.user_play_count import count_user_plays_1h

OBSERVATION_TIME = datetime(2026, 1, 1, 12, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

OBSERVATION_MS = (OBSERVATION_TIME - EPOCH).days * 86_400_000 + (
    OBSERVATION_TIME - EPOCH
).seconds * 1000
WINDOW_START_MS = OBSERVATION_MS - 3_600_000


def make_event(event_time: datetime) -> InteractionEventV1:
    return InteractionEventV1.model_validate(
        {
            "event_id": UUID(int=1),
            "event_time": event_time,
            "ingested_at": event_time,
            "user_id": "user_000001",
            "content_id": "content_000001",
            "event_type": "play",
            "play_seconds": 0,
            "source": "web",
            "schema_version": "v1",
        }
    )


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (timedelta(hours=-1, microseconds=-1), 0),
        (timedelta(hours=-1), 0),
        (timedelta(hours=-1, microseconds=1), 1),
        (timedelta(0), 1),
        (timedelta(microseconds=1), 0),
    ],
)
def test_window_membership_matches_reference(offset: timedelta, expected: int) -> None:
    event = make_event(OBSERVATION_TIME + offset)

    timestamp = right_closed_timestamp_ms(event.event_time)
    window_count = int(WINDOW_START_MS <= timestamp < OBSERVATION_MS)

    reference_count = count_user_plays_1h(
        [event],
        user_id="user_000001",
        observation_time=OBSERVATION_TIME,
    )

    assert window_count == reference_count == expected


def test_equivalent_timezone_offsets_have_the_same_timestamp() -> None:
    local_time = OBSERVATION_TIME.astimezone(timezone(timedelta(hours=2)))

    assert right_closed_timestamp_ms(local_time) == (right_closed_timestamp_ms(OBSERVATION_TIME))


def test_naive_event_time_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        right_closed_timestamp_ms(datetime(2026, 1, 1, 12))
