"""Verify a real Redis WATCH conflict and the store's retry behavior."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from redis import Redis
from redis.exceptions import WatchError

from featureforge_stream.online_store import (
    RedisUserPlayCountStore,
    UserPlayCountFeatureV1,
)


@pytest.mark.integration
def test_concurrent_newer_write_is_preserved_after_watch_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_url = os.environ.get(
        "FEATUREFORGE_REDIS_URL",
        "redis://127.0.0.1:6379/0",
    )
    key_prefix = f"featureforge-stream:integration:{uuid4().hex}"
    user_id = f"integration_user_{uuid4().hex}"
    observation_time = datetime(2026, 1, 1, 12, tzinfo=UTC)

    def make_feature(
        *,
        minute_offset: int,
        play_count: int,
    ) -> UserPlayCountFeatureV1:
        feature_time = observation_time + timedelta(minutes=minute_offset)
        return UserPlayCountFeatureV1(
            user_id=user_id,
            play_count=play_count,
            observation_time=feature_time,
            window_start_exclusive=feature_time - timedelta(hours=1),
        )

    initial = make_feature(minute_offset=0, play_count=1)
    incoming = make_feature(minute_offset=1, play_count=2)
    competing = make_feature(minute_offset=2, play_count=3)

    connection_options = {
        "decode_responses": True,
        "socket_connect_timeout": 3,
        "socket_timeout": 3,
    }

    with (
        Redis.from_url(redis_url, **connection_options) as client,
        Redis.from_url(redis_url, **connection_options) as competing_client,
    ):
        assert client.ping() is True
        assert competing_client.ping() is True

        store = RedisUserPlayCountStore(client, key_prefix=key_prefix)
        key = store.key_for(user_id)

        pipeline_count = 0
        execute_count = 0
        watch_error_count = 0
        competing_write_done = False
        original_pipeline = client.pipeline

        def intercepted_pipeline(*args, **kwargs):
            nonlocal pipeline_count
            pipeline_count += 1

            pipeline = original_pipeline(*args, **kwargs)
            original_execute = pipeline.execute

            def intercepted_execute(*execute_args, **execute_kwargs):
                nonlocal execute_count
                nonlocal watch_error_count
                nonlocal competing_write_done

                execute_count += 1

                if not competing_write_done:
                    competing_client.set(key, competing.model_dump_json())
                    competing_write_done = True

                try:
                    return original_execute(*execute_args, **execute_kwargs)
                except WatchError:
                    watch_error_count += 1
                    raise

            monkeypatch.setattr(pipeline, "execute", intercepted_execute)
            return pipeline

        try:
            assert store.write(initial) == "written"

            monkeypatch.setattr(client, "pipeline", intercepted_pipeline)

            result = store.write(incoming)

            assert competing_write_done is True
            assert watch_error_count == 1
            assert pipeline_count == 2
            assert execute_count == 1
            assert result == "stale"
            assert store.read(user_id) == competing
        finally:
            client.delete(key)
