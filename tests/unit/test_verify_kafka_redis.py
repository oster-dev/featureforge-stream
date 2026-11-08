"""Verify the Kafka-to-Redis checker without external services."""

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException

from featureforge_stream.online_store import UserPlayCountFeatureV1

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "verify_kafka_redis.py"
SPEC = importlib.util.spec_from_file_location("verify_kafka_redis_script", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Cannot load script: {SCRIPT_PATH}")

module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)

PREFIX = "test:replay:user_play_count_1h"
TOPIC = "features.replay.test.user-play-count-1h.v1"


def make_feature(*, user_id="user_1", count=2, minute=0):
    observation = datetime(2026, 1, 1, 12, tzinfo=UTC) + timedelta(minutes=minute)
    return UserPlayCountFeatureV1(
        user_id=user_id,
        play_count=count,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=1),
    )


def make_client(features, extra_keys=()):
    payloads = {f"{PREFIX}:{feature.user_id}": feature.model_dump_json() for feature in features}
    client = Mock()
    client.get.side_effect = payloads.get
    client.scan_iter.return_value = [*payloads, *extra_keys]
    return client


def make_message(feature, *, offset=0):
    message = Mock()
    message.error.return_value = None
    message.topic.return_value = TOPIC
    message.partition.return_value = 0
    message.offset.return_value = offset
    message.value.return_value = feature.model_dump_json().encode()
    return message


def make_consumer(messages, *, low=0, high=None):
    if high is None:
        high = len(messages)

    consumer = Mock()
    consumer.list_topics.return_value = Mock(
        topics={TOPIC: Mock(error=None, partitions={0: Mock()})}
    )
    consumer.get_watermark_offsets.return_value = (low, high)
    consumer.poll.side_effect = messages
    return consumer


def test_select_latest_prefers_newer_window_even_with_lower_count():
    older = make_feature(count=10)
    newer = make_feature(count=0, minute=1)
    expected = {}

    for feature in (older, newer, older):
        module.select_latest(expected, feature)

    assert expected == {"user_1": newer}


def test_select_latest_keeps_highest_count_in_same_window():
    expected = {}
    for count in (2, 5, 1, 5):
        module.select_latest(expected, make_feature(count=count))

    assert expected["user_1"].play_count == 5


def test_select_latest_keeps_users_separate():
    expected = {}
    first = make_feature(user_id="user_1")
    second = make_feature(user_id="user_2")

    module.select_latest(expected, first)
    module.select_latest(expected, second)

    assert expected == {"user_1": first, "user_2": second}


def test_matching_redis_values_pass():
    feature = make_feature()
    client = make_client([feature], extra_keys=["unrelated:key"])

    report = module.compare_redis(client, key_prefix=PREFIX, expected={"user_1": feature})

    assert report["passed"] is True
    assert report["expected_users"] == 1
    assert report["redis_keys"] == 1
    assert report["mismatch_count"] == 0
    assert report["extra_key_count"] == 0
    client.set.assert_not_called()


def test_missing_redis_value_fails():
    feature = make_feature()

    report = module.compare_redis(make_client([]), key_prefix=PREFIX, expected={"user_1": feature})

    assert report["passed"] is False
    assert report["mismatch_count"] == 1
    assert report["mismatch_examples"][0]["actual"] is None


def test_different_redis_value_fails():
    expected = make_feature(count=3)
    actual = make_feature(count=2)

    report = module.compare_redis(
        make_client([actual]), key_prefix=PREFIX, expected={"user_1": expected}
    )

    assert report["passed"] is False
    assert report["mismatch_count"] == 1


def test_extra_namespace_key_fails():
    feature = make_feature()

    report = module.compare_redis(
        make_client([feature], extra_keys=[f"{PREFIX}:unexpected"]),
        key_prefix=PREFIX,
        expected={"user_1": feature},
    )

    assert report["passed"] is False
    assert report["extra_key_count"] == 1


def test_empty_expected_set_is_rejected():
    with pytest.raises(ValueError, match="empty expected"):
        module.compare_redis(Mock(), key_prefix=PREFIX, expected={})


def test_blank_prefix_is_rejected():
    with pytest.raises(ValueError, match="key_prefix"):
        module.compare_redis(Mock(), key_prefix=" ", expected={"user_1": make_feature()})


def test_reads_complete_range_without_committing():
    first = make_feature(count=1)
    corrected = make_feature(count=3)
    consumer = make_consumer([make_message(first), make_message(corrected, offset=1)])

    expected, records, end_offset = module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)

    assert expected == {"user_1": corrected}
    assert records == 2
    assert end_offset == 2
    consumer.commit.assert_not_called()
    consumer.store_offsets.assert_not_called()


@pytest.mark.parametrize("low,high", [(0, 0), (2, 2), (1, 3)])
def test_rejects_empty_or_truncated_range(low, high):
    consumer = make_consumer([], low=low, high=high)

    with pytest.raises(RuntimeError, match="complete non-empty range"):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)

    consumer.assign.assert_not_called()


def test_rejects_offset_gap():
    consumer = make_consumer([make_message(make_feature(), offset=1)])

    with pytest.raises(RuntimeError, match="Unexpected Kafka position"):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)


def test_rejects_tombstone():
    message = make_message(make_feature())
    message.value.return_value = None
    consumer = make_consumer([message])

    with pytest.raises(ValueError, match="Tombstone"):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)


def test_rejects_range_change():
    consumer = make_consumer([make_message(make_feature())])
    consumer.get_watermark_offsets.side_effect = [(0, 1), (0, 2)]

    with pytest.raises(RuntimeError, match="range changed"):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)


def test_rejects_multiple_partitions():
    consumer = make_consumer([])
    consumer.list_topics.return_value.topics[TOPIC].partitions = {
        0: Mock(),
        1: Mock(),
    }

    with pytest.raises(RuntimeError, match="exactly one"):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)


def test_rejects_missing_topic():
    consumer = make_consumer([])
    consumer.list_topics.return_value.topics = {}

    with pytest.raises(RuntimeError, match="not available"):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)


def test_propagates_kafka_message_error():
    message = make_message(make_feature())
    message.error.return_value = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)
    consumer = make_consumer([message])

    with pytest.raises(KafkaException):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)


def test_times_out_without_progress(monkeypatch):
    consumer = make_consumer([None], high=1)
    monkeypatch.setattr(module.time, "monotonic", Mock(side_effect=[0, 0, 11]))

    with pytest.raises(TimeoutError, match="timed out"):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=10)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_rejects_invalid_timeout(timeout):
    consumer = Mock()

    with pytest.raises(ValueError, match="finite and positive"):
        module.read_expected(consumer, topic=TOPIC, timeout_seconds=timeout)

    consumer.list_topics.assert_not_called()
