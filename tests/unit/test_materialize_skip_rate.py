"""Unit tests for Kafka-to-Redis materialization without external services."""

import argparse
from unittest.mock import Mock

import pytest

pytest.importorskip("confluent_kafka")


from featureforge_stream import materialize_skip_rate as module
from featureforge_stream.contracts.user_skip_rate import (
    SkipRateUpdateConflictError,
    UserSkipRateFeatureV1,
)


def make_arguments(**overrides):
    values = {
        "bootstrap_servers": "127.0.0.1:9092",
        "redis_url": "redis://127.0.0.1:6379/0",
        "topic": "features.replay.test.user-skip-rate-24h.v1",
        "group_id": "materializer-unit-test",
        "key_prefix": "featureforge-stream:test:user_skip_rate_24h",
        "commit_every": 1000,
        "bounded": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def feature_payload():
    return (
        UserSkipRateFeatureV1(
            user_id="user-001",
            play_count=3,
            skip_count=2,
            skip_rate=0.4,
            observation_time="2026-03-01T12:00:00Z",
            window_start_exclusive="2026-02-28T12:00:00Z",
        )
        .model_dump_json()
        .encode()
    )


class FakeMessage:
    def __init__(self, offset, payload=None):
        self._offset = offset
        self._payload = feature_payload() if payload is None else payload

    def offset(self):
        return self._offset

    def partition(self):
        return 0

    def value(self):
        return self._payload

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, *, low, high, committed, messages, actions):
        self.low = low
        self.high = high
        self.committed_offset = committed
        self.messages = iter(messages)
        self.actions = actions
        self.assignments = []
        self.commits = []
        self.closed = False

    def list_topics(self, topic, timeout):
        partition_metadata = Mock()
        topic_metadata = Mock(
            error=None,
            partitions={0: partition_metadata},
        )
        return Mock(topics={topic: topic_metadata})

    def get_watermark_offsets(self, partition, timeout, cached):
        return self.low, self.high

    def committed(self, partitions, timeout):
        return [Mock(offset=self.committed_offset, error=None)]

    def assign(self, partitions):
        self.assignments.extend(partitions)

    def poll(self, timeout):
        try:
            return next(self.messages)
        except StopIteration as error:
            raise AssertionError("Materializer polled beyond the supplied test messages") from error

    def commit(self, *, offsets, asynchronous):
        assert asynchronous is False
        next_offset = offsets[0].offset
        self.commits.append(next_offset)
        self.actions.append(("commit", next_offset))
        return []

    def close(self):
        self.closed = True


class FakeStore:
    def __init__(self, actions, outcomes):
        self.actions = actions
        self.outcomes = iter(outcomes)
        self.features = []

    def write(self, feature):
        self.actions.append(("write", feature.user_id))
        self.features.append(feature)

        try:
            outcome = next(self.outcomes)
        except StopIteration as error:
            raise AssertionError("Unexpected additional Redis write") from error

        if isinstance(outcome, Exception):
            raise outcome

        return outcome


@pytest.fixture
def harness(monkeypatch):
    def build(
        *,
        low=0,
        high=1,
        committed=-1001,
        messages=None,
        outcomes=None,
    ):
        actions = []
        consumer = FakeConsumer(
            low=low,
            high=high,
            committed=committed,
            messages=[FakeMessage(0)] if messages is None else messages,
            actions=actions,
        )
        store = FakeStore(
            actions,
            ["written"] if outcomes is None else outcomes,
        )

        redis_client = Mock()
        redis_client.ping.return_value = True
        redis_context = Mock()
        redis_context.__enter__ = Mock(return_value=redis_client)
        redis_context.__exit__ = Mock(return_value=False)

        redis_factory = Mock(return_value=redis_context)
        consumer_factory = Mock(return_value=consumer)
        store_factory = Mock(return_value=store)

        monkeypatch.setattr(module.Redis, "from_url", redis_factory)
        monkeypatch.setattr(module, "Consumer", consumer_factory)
        monkeypatch.setattr(module, "RedisUserSkipRateStore", store_factory)

        return argparse.Namespace(
            actions=actions,
            consumer=consumer,
            store=store,
            redis_client=redis_client,
            redis_factory=redis_factory,
            consumer_factory=consumer_factory,
        )

    return build


def test_writes_before_committing_and_uses_ipv4(harness):
    state = harness()

    results = module.materialize(make_arguments(commit_every=1))

    assert dict(results) == {"written": 1}
    assert state.actions == [
        ("write", "user-001"),
        ("commit", 1),
    ]
    assert state.consumer.closed is True

    configuration = state.consumer_factory.call_args.args[0]
    assert configuration["broker.address.family"] == "v4"
    assert configuration["enable.auto.commit"] is False
    assert configuration["enable.auto.offset.store"] is False


def test_failed_write_does_not_commit_failed_record(harness):
    state = harness(
        high=2,
        messages=[FakeMessage(0), FakeMessage(1)],
        outcomes=["written", RuntimeError("Redis write failed")],
    )

    with pytest.raises(RuntimeError, match="Redis write failed"):
        module.materialize(make_arguments(commit_every=1))

    assert state.consumer.commits == [1]
    assert state.consumer.closed is True


def test_bounded_run_stops_at_snapshot_end(harness):
    state = harness(
        low=10,
        high=12,
        messages=[
            FakeMessage(10),
            FakeMessage(11),
            FakeMessage(12),
        ],
        outcomes=["written", "written", "written"],
    )

    results = module.materialize(make_arguments())

    assert dict(results) == {"written": 2}
    assert len(state.store.features) == 2
    assert state.consumer.commits == [12]
    assert state.consumer.assignments[0].offset == 10
    assert state.consumer.closed is True


def test_resumes_from_committed_offset(harness):
    state = harness(
        low=0,
        high=12,
        committed=11,
        messages=[FakeMessage(11)],
    )

    results = module.materialize(make_arguments())

    assert state.consumer.assignments[0].offset == 11
    assert state.consumer.commits == [12]
    assert dict(results) == {"written": 1}


def test_starts_at_low_watermark_without_committed_offset(harness):
    state = harness(
        low=20,
        high=21,
        committed=-1001,
        messages=[FakeMessage(20)],
    )

    module.materialize(make_arguments())

    assert state.consumer.assignments[0].offset == 20
    assert state.consumer.commits == [21]


def test_empty_topic_raises_and_closes_consumer(harness):
    state = harness(
        low=20,
        high=20,
        messages=[],
        outcomes=[],
    )

    with pytest.raises(RuntimeError, match="No readable records"):
        module.materialize(make_arguments())

    assert state.consumer.assignments == []
    assert state.consumer.commits == []
    assert state.store.features == []
    assert state.consumer.closed is True


def test_caught_up_group_returns_without_writing(harness):
    state = harness(
        low=0,
        high=10,
        committed=10,
        messages=[],
        outcomes=[],
    )

    results = module.materialize(make_arguments())

    assert dict(results) == {}
    assert state.consumer.assignments == []
    assert state.consumer.commits == []
    assert state.consumer.closed is True


def test_counts_all_store_outcomes_and_commits_final_partial_batch(harness):
    state = harness(
        high=3,
        messages=[
            FakeMessage(0),
            FakeMessage(1),
            FakeMessage(2),
        ],
        outcomes=["written", "unchanged", "stale"],
    )

    results = module.materialize(make_arguments(commit_every=2))

    assert dict(results) == {
        "written": 1,
        "unchanged": 1,
        "stale": 1,
    }
    assert state.consumer.commits == [2, 3]


def test_invalid_payload_is_not_committed(harness):
    state = harness(
        messages=[FakeMessage(0, payload=b"not-json")],
        outcomes=[],
    )

    with pytest.raises(ValueError):
        module.materialize(make_arguments())

    assert state.consumer.commits == []
    assert state.store.features == []
    assert state.consumer.closed is True


def test_invalid_commit_interval_fails_before_connections(harness):
    state = harness()

    with pytest.raises(ValueError, match="must be positive"):
        module.materialize(make_arguments(commit_every=0))

    state.redis_factory.assert_not_called()
    state.consumer_factory.assert_not_called()


def test_commit_offset_uses_next_offset_and_synchronous_commit():
    consumer = Mock()
    consumer.commit.return_value = []
    partition = module.TopicPartition("features.test.v1", 0)

    module.commit_offset(consumer, partition, 42)

    arguments = consumer.commit.call_args.kwargs
    assert arguments["asynchronous"] is False
    assert len(arguments["offsets"]) == 1
    assert arguments["offsets"][0].topic == "features.test.v1"
    assert arguments["offsets"][0].partition == 0
    assert arguments["offsets"][0].offset == 42


def test_count_conflict_does_not_commit_failed_record(harness):
    state = harness(
        high=2,
        messages=[FakeMessage(0), FakeMessage(1)],
        outcomes=["written", SkipRateUpdateConflictError("Incomparable counts")],
    )

    with pytest.raises(SkipRateUpdateConflictError, match="Incomparable"):
        module.materialize(make_arguments(commit_every=1))

    assert state.consumer.commits == [1]
    assert state.consumer.closed is True


def test_rejects_inconsistent_skip_rate_before_store_write(harness):
    feature = UserSkipRateFeatureV1.model_validate_json(feature_payload())
    payload = feature.model_dump(mode="json")
    payload["skip_rate"] = 0.9

    import json

    state = harness(
        messages=[FakeMessage(0, payload=json.dumps(payload).encode())],
        outcomes=[],
    )

    with pytest.raises(ValueError, match="skip_rate"):
        module.materialize(make_arguments())

    assert state.consumer.commits == []
    assert state.store.features == []
    assert state.consumer.closed is True
