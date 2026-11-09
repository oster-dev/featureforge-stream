"""Test source/snapshot script readers and snapshot integrity checks."""

import hashlib
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock
from uuid import UUID

import pytest
from confluent_kafka import KafkaError, KafkaException

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.online_store import UserPlayCountFeatureV1
from featureforge_stream.snapshot import write_snapshot

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
TOPIC = "test.replay.input"


def load_script(name):
    spec = importlib.util.spec_from_file_location(f"{name}_test_target", SCRIPTS / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def scripts(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS))
    return {
        "source": load_script("verify_source_snapshot"),
        "late": load_script("reconcile_source_snapshot"),
    }


@pytest.fixture(params=["source", "late"])
def reader(request, scripts):
    kind = request.param
    module = scripts[kind]
    function = module.read_events if kind == "source" else module.read_late_records
    return kind, module, function


def event():
    timestamp = datetime(2026, 1, 1, 12, tzinfo=UTC)
    return InteractionEventV1(
        event_id=UUID(int=1),
        event_time=timestamp,
        ingested_at=timestamp,
        user_id="user_1",
        content_id="content_1",
        event_type="play",
        play_seconds=10,
        source="web",
        schema_version="v1",
    )


def payload(kind):
    if kind == "source":
        return event().model_dump_json().encode()
    return json.dumps(
        {
            "error_schema_version": "v1",
            "feature_name": "user_play_count_1h",
            "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
            "raw_payload": event().model_dump_json(),
        }
    ).encode()


def message(kind, *, offset=0):
    record = Mock()
    record.error.return_value = None
    record.topic.return_value = TOPIC
    record.partition.return_value = 0
    record.offset.return_value = offset
    record.value.return_value = payload(kind)
    return record


def consumer_for(messages, *, low=0, high=None):
    consumer = Mock()
    consumer.list_topics.return_value = Mock(
        topics={TOPIC: Mock(error=None, partitions={0: Mock()})}
    )
    consumer.get_watermark_offsets.return_value = (low, len(messages) if high is None else high)
    consumer.poll.side_effect = messages
    return consumer


def test_readers_capture_complete_range_without_committing(reader):
    kind, _, read = reader
    consumer = consumer_for([message(kind)])

    records, source = read(consumer, topic=TOPIC, timeout_seconds=10)

    assert len(records) == 1
    assert source["start_offset_inclusive"] == 0
    assert source["end_offset_exclusive"] == 1
    assert source["records_read"] == 1
    digest_field = "normalized_events_sha256" if kind == "source" else "normalized_envelopes_sha256"
    assert len(source[digest_field]) == 64
    consumer.commit.assert_not_called()
    consumer.store_offsets.assert_not_called()


def test_readers_reject_truncated_range(reader):
    _, _, read = reader
    consumer = consumer_for([], low=1, high=2)

    with pytest.raises(RuntimeError):
        read(consumer, topic=TOPIC, timeout_seconds=10)

    consumer.assign.assert_not_called()


def test_readers_reject_missing_topic(reader):
    _, _, read = reader
    consumer = consumer_for([])
    consumer.list_topics.return_value.topics = {}

    with pytest.raises(RuntimeError, match="unavailable"):
        read(consumer, topic=TOPIC, timeout_seconds=10)


def test_readers_reject_multiple_partitions(reader):
    _, _, read = reader
    consumer = consumer_for([])
    consumer.list_topics.return_value.topics[TOPIC].partitions = {0: Mock(), 1: Mock()}

    with pytest.raises(RuntimeError, match="one"):
        read(consumer, topic=TOPIC, timeout_seconds=10)


@pytest.mark.parametrize(
    "field,value",
    [("offset", 1), ("topic", "wrong"), ("partition", 1)],
)
def test_readers_reject_unexpected_position(reader, field, value):
    kind, _, read = reader
    record = message(kind)
    getattr(record, field).return_value = value

    with pytest.raises(RuntimeError, match="Unexpected"):
        read(consumer_for([record]), topic=TOPIC, timeout_seconds=10)


def test_readers_reject_tombstones(reader):
    kind, _, read = reader
    record = message(kind)
    record.value.return_value = None

    with pytest.raises(ValueError, match="[Tt]ombstone"):
        read(consumer_for([record]), topic=TOPIC, timeout_seconds=10)


def test_readers_propagate_kafka_errors(reader):
    kind, _, read = reader
    record = message(kind)
    record.error.return_value = KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)

    with pytest.raises(KafkaException):
        read(consumer_for([record]), topic=TOPIC, timeout_seconds=10)


def test_readers_reject_changed_range(reader):
    kind, _, read = reader
    consumer = consumer_for([message(kind)])
    consumer.get_watermark_offsets.side_effect = [(0, 1), (0, 2)]

    with pytest.raises(RuntimeError, match="range changed"):
        read(consumer, topic=TOPIC, timeout_seconds=10)


def test_readers_time_out_without_progress(reader, monkeypatch):
    _, module, read = reader
    consumer = consumer_for([None], high=1)
    monkeypatch.setattr(module.time, "monotonic", Mock(side_effect=[0, 0, 11]))

    with pytest.raises(TimeoutError):
        read(consumer, topic=TOPIC, timeout_seconds=10)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_readers_reject_invalid_timeout(reader, timeout):
    _, _, read = reader
    consumer = Mock()

    with pytest.raises(ValueError, match="finite and positive"):
        read(consumer, topic=TOPIC, timeout_seconds=timeout)

    consumer.list_topics.assert_not_called()


def test_empty_source_is_rejected_but_empty_late_output_is_valid(scripts):
    with pytest.raises(RuntimeError, match="non-empty"):
        scripts["source"].read_events(consumer_for([]), topic=TOPIC, timeout_seconds=10)

    records, source = scripts["late"].read_late_records(
        consumer_for([]), topic=TOPIC, timeout_seconds=10
    )
    assert records == []
    assert source["records_read"] == 0
    assert source["normalized_envelopes_sha256"] == hashlib.sha256(b"").hexdigest()


def test_late_reader_rejects_non_object_json(scripts):
    record = message("late")
    record.value.return_value = b"[]"

    with pytest.raises(TypeError, match="JSON objects"):
        scripts["late"].read_late_records(consumer_for([record]), topic=TOPIC, timeout_seconds=10)


@pytest.fixture
def snapshot_export(tmp_path):
    path = tmp_path / "windows.parquet"
    observation = event().event_time
    feature = UserPlayCountFeatureV1(
        user_id="user_1",
        play_count=1,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=1),
    )
    write_snapshot([feature], output_path=path)
    report = {
        "export_contract": "featureforge-stream.replay-snapshot-export.v1",
        "comparison": {"passed": True},
        "snapshot": {
            "file": "windows.parquet",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "window_rows": 1,
            "restored_rows": 1,
        },
        "source": {"topic": "test.features", "records_read": 1},
    }
    (tmp_path / "verification.json").write_text(json.dumps(report))
    return tmp_path, feature, report


def test_load_verified_snapshot_checks_real_parquet(scripts, snapshot_export):
    directory, feature, report = snapshot_export

    windows, metadata = scripts["source"].load_verified_snapshot(directory)

    assert windows == [feature]
    assert metadata["sha256"] == report["snapshot"]["sha256"]
    assert metadata["feature_output_source"] == report["source"]


@pytest.mark.parametrize(
    "section,field,value",
    [
        (None, "export_contract", "unsupported"),
        ("comparison", "passed", False),
        ("snapshot", "file", "../other.parquet"),
        ("snapshot", "sha256", "wrong"),
        ("snapshot", "window_rows", 2),
        ("snapshot", "restored_rows", 2),
    ],
)
def test_load_rejects_invalid_export_report(scripts, snapshot_export, section, field, value):
    directory, _, report = snapshot_export
    target = report if section is None else report[section]
    target[field] = value
    (directory / "verification.json").write_text(json.dumps(report))

    with pytest.raises(ValueError):
        scripts["source"].load_verified_snapshot(directory)


def test_load_rejects_changed_snapshot_bytes(scripts, snapshot_export):
    directory, _, _ = snapshot_export
    with (directory / "windows.parquet").open("ab") as target:
        target.write(b"changed")

    with pytest.raises(ValueError, match="SHA-256"):
        scripts["source"].load_verified_snapshot(directory)


@pytest.mark.parametrize("kind", ["source", "late"])
def test_cli_closes_consumer_on_input_failure(
    scripts, snapshot_export, monkeypatch, tmp_path, kind
):
    module = scripts[kind]
    directory, _, _ = snapshot_export
    consumer = Mock()
    monkeypatch.setattr(module, "Consumer", Mock(return_value=consumer))
    monkeypatch.setattr(module, "read_events", Mock(side_effect=RuntimeError("input failure")))
    report = tmp_path / "new-report.json"
    arguments = [
        "checker",
        "--input-topic",
        TOPIC,
        "--snapshot-dir",
        str(directory),
        "--report",
        str(report),
    ]
    if kind == "late":
        arguments.extend(["--late-topic", "test.late"])
    monkeypatch.setattr(sys, "argv", arguments)

    with pytest.raises(RuntimeError, match="input failure"):
        module.main()

    consumer.close.assert_called_once()
    assert not report.exists()
