"""Verify feature round trips against the local Redis service."""

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from redis import Redis

from featureforge_stream.online_store import (
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


@pytest.mark.integration
@pytest.mark.parametrize("play_count", [0, 3])
def test_feature_round_trip(
    online_store: RedisUserPlayCountStore,
    play_count: int,
) -> None:
    observation_time = datetime(2026, 1, 1, 12, tzinfo=UTC)
    feature = UserPlayCountFeatureV1(
        user_id=f"integration_user_{uuid4().hex}",
        play_count=play_count,
        observation_time=observation_time,
        window_start_exclusive=observation_time - timedelta(hours=1),
    )

    online_store.write(feature)

    actual = online_store.read(feature.user_id)

    assert actual == feature


@pytest.mark.integration
def test_unknown_user_returns_none(
    online_store: RedisUserPlayCountStore,
) -> None:
    actual = online_store.read(f"unknown_user_{uuid4().hex}")

    assert actual is None
