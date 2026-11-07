"""Verify guarded feature writes against the local Redis service."""

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from redis import Redis

from featureforge_stream.online_store import (
    FeatureWriteConflictError,
    RedisUserPlayCountStore,
    UserPlayCountFeatureV1,
)


@pytest.fixture
def online_store() -> Iterator[RedisUserPlayCountStore]:
    redis_url = os.environ.get(
        "FEATUREFORGE_REDIS_URL",
        "redis://127.0.0.1:6379/0",
    )
    key_prefix = f"featureforge-stream:integration:{uuid4().hex}"

    with Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as client:
        assert client.ping() is True
        store = RedisUserPlayCountStore(client, key_prefix=key_prefix)

        try:
            yield store
        finally:
            keys = list(client.scan_iter(match=f"{key_prefix}:*"))
            if keys:
                client.delete(*keys)


def make_feature(
    *,
    user_id: str,
    play_count: int,
    observation_time: datetime,
) -> UserPlayCountFeatureV1:
    return UserPlayCountFeatureV1(
        user_id=user_id,
        play_count=play_count,
        observation_time=observation_time,
        window_start_exclusive=observation_time - timedelta(hours=1),
    )


@pytest.mark.integration
@pytest.mark.parametrize("play_count", [0, 3])
def test_feature_round_trip(
    online_store: RedisUserPlayCountStore,
    play_count: int,
) -> None:
    feature = make_feature(
        user_id=f"integration_user_{uuid4().hex}",
        play_count=play_count,
        observation_time=datetime(2026, 1, 1, 12, tzinfo=UTC),
    )

    assert online_store.write(feature) == "written"
    assert online_store.read(feature.user_id) == feature


@pytest.mark.integration
def test_unknown_user_returns_none(
    online_store: RedisUserPlayCountStore,
) -> None:
    actual = online_store.read(f"unknown_user_{uuid4().hex}")

    assert actual is None


@pytest.mark.integration
def test_newer_window_replaces_stored_feature(
    online_store: RedisUserPlayCountStore,
) -> None:
    user_id = f"integration_user_{uuid4().hex}"
    observation_time = datetime(2026, 1, 1, 12, tzinfo=UTC)

    initial = make_feature(
        user_id=user_id,
        play_count=3,
        observation_time=observation_time,
    )
    newer = make_feature(
        user_id=user_id,
        play_count=1,
        observation_time=observation_time + timedelta(minutes=1),
    )

    assert online_store.write(initial) == "written"
    assert online_store.write(newer) == "written"
    assert online_store.read(user_id) == newer


@pytest.mark.integration
def test_older_window_cannot_replace_newer_feature(
    online_store: RedisUserPlayCountStore,
) -> None:
    user_id = f"integration_user_{uuid4().hex}"
    observation_time = datetime(2026, 1, 1, 12, tzinfo=UTC)

    current = make_feature(
        user_id=user_id,
        play_count=3,
        observation_time=observation_time,
    )
    older = make_feature(
        user_id=user_id,
        play_count=99,
        observation_time=observation_time - timedelta(minutes=1),
    )

    assert online_store.write(current) == "written"
    assert online_store.write(older) == "stale"
    assert online_store.read(user_id) == current


@pytest.mark.integration
def test_identical_delivery_is_unchanged(
    online_store: RedisUserPlayCountStore,
) -> None:
    feature = make_feature(
        user_id=f"integration_user_{uuid4().hex}",
        play_count=3,
        observation_time=datetime(2026, 1, 1, 12, tzinfo=UTC),
    )

    assert online_store.write(feature) == "written"

    for _ in range(3):
        assert online_store.write(feature) == "unchanged"

    assert online_store.read(feature.user_id) == feature


@pytest.mark.integration
@pytest.mark.parametrize("conflicting_count", [2, 4])
def test_same_window_with_different_value_is_explicit_conflict(
    online_store: RedisUserPlayCountStore,
    conflicting_count: int,
) -> None:
    user_id = f"integration_user_{uuid4().hex}"
    observation_time = datetime(2026, 1, 1, 12, tzinfo=UTC)

    initial = make_feature(
        user_id=user_id,
        play_count=3,
        observation_time=observation_time,
    )
    conflicting = make_feature(
        user_id=user_id,
        play_count=conflicting_count,
        observation_time=observation_time,
    )

    assert online_store.write(initial) == "written"

    with pytest.raises(
        FeatureWriteConflictError,
        match="Different feature values for the same observation_time",
    ):
        online_store.write(conflicting)

    assert online_store.read(user_id) == initial
