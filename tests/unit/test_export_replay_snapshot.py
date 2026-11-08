"""Test bounded Kafka export and its persisted verification report."""

import hashlib
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException

from featureforge_stream.online_store import UserPlayCountFeatureV1
from featureforge_stream.snapshot import read_snapshot

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
TOPIC = "features.replay.test.user-play-count-1h.v1"
PREFIX = "test:replay:user_play_count_1h"


@pytest.fixture
def exporter(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(
        "export_replay_snapshot_test_target",
        SCRIPTS / "export_replay_snapshot.py",
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load snapshot exporter")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_feature(*, count=2, minute=0):
    observation = datetime(2026, 1, 1, 12, tzinfo=UTC) + timedelta(minutes=minute)
    return UserPlayCountFeatureV1(
        user_id="user_1",
        play_count=count,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=1),
    )


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


def test_reader_preserves_updates_and_historical_windows(exporter):
    features = [
        make_feature(count=1),
        make_feature(count=3),
        make_feature(count=0, minute=1),
    ]
    consumer = make_consumer(
        [make_message(feature, offset=index) for index, feature in enumerate(features)]
    )

    restored, source = exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=10)

    assert restored == features
    assert source == {
        "topic": TOPIC,
        "partition": 0,
        "start_offset_inclusive": 0,
        "end_offset_exclusive": 3,
        "records_read": 3,
    }
    assignment = consumer.assign.call_args.args[0]
    assert len(assignment) == 1
    assert assignment[0].topic == TOPIC
    assert assignment[0].partition == 0
    assert assignment[0].offset == 0
    consumer.commit.assert_not_called()
    consumer.store_offsets.assert_not_called()


@pytest.mark.parametrize("low,high", [(0, 0), (1, 2), (2, 2)])
def test_reader_rejects_empty_or_truncated_range(exporter, low, high):
    consumer = make_consumer([], low=low, high=high)

    with pytest.raises(RuntimeError, match="complete non-empty range"):
        exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=10)

    consumer.assign.assert_not_called()


def test_reader_rejects_missing_topic(exporter):
    consumer = make_consumer([])
    consumer.list_topics.return_value.topics = {}

    with pytest.raises(RuntimeError, match="not available"):
        exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=10)


def test_reader_rejects_topic_metadata_error(exporter):
    consumer = make_consumer([])
    consumer.list_topics.return_value.topics[TOPIC].error = Mock()

    with pytest.raises(RuntimeError, match="not available"):
        exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=10)


def test_reader_rejects_multiple_partitions(exporter):
    consumer = make_consumer([])
    consumer.list_topics.return_value.topics[TOPIC].partitions = {
        0: Mock(),
        1: Mock(),
    }

    with pytest.raises(RuntimeError, match="exactly one"):
        exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=10)


@pytest.mark.parametrize(
    "field,value",
    [("offset", 1), ("topic", "wrong-topic"), ("partition", 1)],
)
def test_reader_rejects_unexpected_position(exporter, field, value):
    message = make_message(make_feature())
    getattr(message, field).return_value = value
    consumer = make_consumer([message])

    with pytest.raises(RuntimeError, match="Unexpected Kafka position"):
        exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=10)


def test_reader_rejects_tombstone(exporter):
    message = make_message(make_feature())
    message.value.return_value = None

    with pytest.raises(ValueError, match="Tombstone"):
        exporter.read_windows(make_consumer([message]), topic=TOPIC, timeout_seconds=10)


def test_reader_rejects_invalid_feature_payload(exporter):
    message = make_message(make_feature())
    message.value.return_value = b'{"user_id": "user_1"}'

    with pytest.raises(ValueError):
        exporter.read_windows(make_consumer([message]), topic=TOPIC, timeout_seconds=10)


def test_reader_propagates_kafka_error(exporter):
    message = make_message(make_feature())
    message.error.return_value = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)

    with pytest.raises(KafkaException):
        exporter.read_windows(make_consumer([message]), topic=TOPIC, timeout_seconds=10)


def test_reader_rejects_changed_range(exporter):
    consumer = make_consumer([make_message(make_feature())])
    consumer.get_watermark_offsets.side_effect = [(0, 1), (0, 2)]

    with pytest.raises(RuntimeError, match="range changed"):
        exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=10)


def test_reader_times_out_without_progress(exporter, monkeypatch):
    consumer = make_consumer([None], high=1)
    monkeypatch.setattr(exporter.time, "monotonic", Mock(side_effect=[0, 0, 11]))

    with pytest.raises(TimeoutError, match="timed out"):
        exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=10)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_reader_rejects_invalid_timeout(exporter, timeout):
    consumer = Mock()

    with pytest.raises(ValueError, match="finite and positive"):
        exporter.read_windows(consumer, topic=TOPIC, timeout_seconds=timeout)

    consumer.list_topics.assert_not_called()


def configure_cli(exporter, monkeypatch, output, consumer):
    factory = Mock(return_value=consumer)
    monkeypatch.setattr(exporter, "Consumer", factory)

    client = Mock()
    context = Mock()
    context.__enter__ = Mock(return_value=client)
    context.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(exporter.Redis, "from_url", Mock(return_value=context))

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "export_replay_snapshot.py",
            "--topic",
            TOPIC,
            "--key-prefix",
            PREFIX,
            "--output-dir",
            str(output),
        ],
    )
    return factory, client


@pytest.mark.parametrize("passed", [True, False])
def test_cli_persists_snapshot_hash_and_comparison(exporter, monkeypatch, tmp_path, passed):
    features = [
        make_feature(count=1),
        make_feature(count=3),
        make_feature(count=0, minute=1),
    ]
    consumer = make_consumer(
        [make_message(feature, offset=index) for index, feature in enumerate(features)]
    )
    output = tmp_path / "export"
    factory, client = configure_cli(exporter, monkeypatch, output, consumer)
    comparison = {
        "expected_users": 1,
        "redis_keys": 1,
        "mismatch_count": 0 if passed else 1,
        "extra_key_count": 0,
        "passed": passed,
    }
    compare = Mock(return_value=comparison)
    monkeypatch.setattr(exporter, "compare_redis", compare)

    if passed:
        exporter.main()
    else:
        with pytest.raises(SystemExit, match="persisted snapshot and Redis differ"):
            exporter.main()

    snapshot = output / "windows.parquet"
    report = json.loads((output / "verification.json").read_text())

    assert read_snapshot(snapshot) == [features[1], features[2]]
    assert report["snapshot"]["window_rows"] == 2
    assert report["snapshot"]["restored_rows"] == 2
    assert report["snapshot"]["sha256"] == hashlib.sha256(snapshot.read_bytes()).hexdigest()
    assert report["source"]["records_read"] == 3
    assert report["source"]["end_offset_exclusive"] == 3
    assert report["comparison"] == comparison
    assert report["redis_key_prefix"] == PREFIX
    assert report["started_at"]
    assert report["completed_at"]

    compare.assert_called_once_with(
        client,
        key_prefix=PREFIX,
        expected={"user_1": features[2]},
    )
    config = factory.call_args.args[0]
    assert config["enable.auto.commit"] is False
    assert config["enable.auto.offset.store"] is False
    assert config["allow.auto.create.topics"] is False
    consumer.close.assert_called_once()


def test_cli_closes_consumer_on_read_failure(exporter, monkeypatch, tmp_path):
    consumer = make_consumer([], high=0)
    output = tmp_path / "export"
    configure_cli(exporter, monkeypatch, output, consumer)

    with pytest.raises(RuntimeError, match="complete non-empty range"):
        exporter.main()

    consumer.close.assert_called_once()
    assert not output.exists()


def test_cli_rejects_existing_output_before_connecting(exporter, monkeypatch, tmp_path):
    output = tmp_path / "export"
    output.mkdir()
    marker = output / "existing.txt"
    marker.write_text("keep")
    factory, _ = configure_cli(exporter, monkeypatch, output, Mock())

    with pytest.raises(SystemExit) as error:
        exporter.main()

    assert error.value.code == 2
    factory.assert_not_called()
    assert marker.read_text() == "keep"
