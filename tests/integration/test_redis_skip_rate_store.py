"""Real Redis proofs for skip-rate updates and concurrent-write retries."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from redis import Redis
from redis.exceptions import WatchError

from featureforge_stream.contracts.user_skip_rate import (
    SkipRateUpdateConflictError,
    UserSkipRateFeatureV1,
)
from featureforge_stream.skip_rate_store import RedisUserSkipRateStore

pytestmark = pytest.mark.integration
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)


def make_feature(plays=1, skips=1, *, minutes=0, user_id="user_000001"):
    observation = OBSERVATION + timedelta(minutes=minutes)
    denominator = plays + skips
    return UserSkipRateFeatureV1(
        user_id=user_id,
        play_count=plays,
        skip_count=skips,
        skip_rate=skips / denominator if denominator else 0.0,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=24),
    )


@pytest.fixture
def redis_case():
    url = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
    prefix = f"featureforge-stream:integration:skip-rate:{uuid4().hex}"
    with Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as client:
        assert client.ping() is True
        store = RedisUserSkipRateStore(client, key_prefix=prefix)
        try:
            yield client, store, url
        finally:
            keys = list(client.scan_iter(match=f"{prefix}:*"))
            if keys:
                client.delete(*keys)


@pytest.mark.parametrize(("plays", "skips"), [(0, 0), (1, 0), (0, 1), (2, 1)])
def test_round_trip(redis_case, plays, skips):
    _, store, _ = redis_case
    feature = make_feature(plays, skips)
    assert store.write(feature) == "written"
    assert store.read(feature.user_id) == feature


def test_unknown_user_returns_none(redis_case):
    _, store, _ = redis_case
    assert store.read("unknown_user") is None


def test_downward_and_upward_rate_corrections_and_replay(redis_case):
    _, store, _ = redis_case
    initial = make_feature(1, 1)
    downward = make_feature(2, 1)
    upward = make_feature(2, 2)

    for feature in (initial, downward, upward):
        assert store.write(feature) == "written"
        assert store.read(feature.user_id) == feature

    assert store.write(initial) == "stale"
    assert store.write(downward) == "stale"
    assert store.write(upward) == "unchanged"
    assert store.read(initial.user_id) == upward


def test_window_ordering_and_zero_replacement(redis_case):
    _, store, _ = redis_case
    initial = make_feature(5, 5)
    newer = make_feature(0, 0, minutes=1)
    assert store.write(initial) == "written"
    assert store.write(newer) == "written"
    assert store.write(make_feature(99, 99)) == "stale"
    assert store.read(initial.user_id) == newer


@pytest.mark.parametrize(("plays", "skips"), [(2, 0), (0, 2)])
def test_conflict_leaves_stored_value_unchanged(redis_case, plays, skips):
    client, store, _ = redis_case
    initial = make_feature()
    assert store.write(initial) == "written"
    key = store.key_for(initial.user_id)
    before = client.get(key)

    with pytest.raises(SkipRateUpdateConflictError):
        store.write(make_feature(plays, skips))

    assert client.get(key) == before
    assert store.read(initial.user_id) == initial


@pytest.mark.parametrize("corruption", ["invalid-json", "different-user"])
def test_corrupt_stored_value_is_not_overwritten(redis_case, corruption):
    client, store, _ = redis_case
    feature = make_feature()
    key = store.key_for(feature.user_id)
    payload = (
        "not-json"
        if corruption == "invalid-json"
        else make_feature(user_id="other_user").model_dump_json()
    )
    client.set(key, payload)

    with pytest.raises(ValueError):
        store.write(feature)
    assert client.get(key) == payload
    with pytest.raises(ValueError):
        store.read(feature.user_id)


def test_real_watch_conflict_rechecks_same_window_counts(redis_case, monkeypatch):
    client, store, url = redis_case
    initial = make_feature(1, 1)
    incoming = make_feature(2, 1)
    competing = make_feature(2, 2)
    assert store.write(initial) == "written"
    key = store.key_for(initial.user_id)
    original_pipeline = client.pipeline
    counters = {"pipelines": 0, "executes": 0, "watch_errors": 0}

    with Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as other:
        assert other.ping() is True

        def intercepted_pipeline(*args, **kwargs):
            counters["pipelines"] += 1
            pipeline = original_pipeline(*args, **kwargs)
            original_execute = pipeline.execute

            def intercepted_execute(*execute_args, **execute_kwargs):
                counters["executes"] += 1
                if counters["executes"] == 1:
                    other.set(key, competing.model_dump_json())
                try:
                    return original_execute(*execute_args, **execute_kwargs)
                except WatchError:
                    counters["watch_errors"] += 1
                    raise

            monkeypatch.setattr(pipeline, "execute", intercepted_execute)
            return pipeline

        monkeypatch.setattr(client, "pipeline", intercepted_pipeline)
        assert store.write(incoming) == "stale"

    assert counters == {"pipelines": 2, "executes": 1, "watch_errors": 1}
    assert store.read(initial.user_id) == competing
