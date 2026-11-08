"""Verify isolated replay topic setup without contacting Kafka."""

import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from confluent_kafka import KafkaError, KafkaException

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "create_replay_topics.py"
SPEC = importlib.util.spec_from_file_location("create_replay_topics_script", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Cannot load script: {SCRIPT_PATH}")

module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


@pytest.mark.parametrize("run_name", ["a", "run-01", "a" * 80])
def test_valid_run_names(run_name):
    topics = module.topic_names(run_name)

    assert len(topics) == 5
    assert len(set(topics.values())) == 5
    assert topics["input"] == f"interaction-events.replay.{run_name}.v1"
    assert topics["validated"] == f"interaction-events.replay.{run_name}.validated.v1"
    assert topics["rejected"] == f"interaction-events.replay.{run_name}.rejected.v1"
    assert topics["features"] == f"features.replay.{run_name}.user-play-count-1h.v1"
    assert topics["late"] == f"interaction-events.replay.{run_name}.late.v1"


@pytest.mark.parametrize(
    "run_name",
    ["", "-run", "Run", "run_name", "run.name", "run name", "../run", "a" * 81],
)
def test_invalid_run_names(run_name):
    with pytest.raises(ValueError):
        module.topic_names(run_name)


def test_topic_policy():
    assert module.TOPIC_CONFIG == {
        "cleanup.policy": "delete",
        "retention.ms": "604800000",
        "retention.bytes": "-1",
        "message.timestamp.type": "LogAppendTime",
    }


def test_preview_does_not_connect(monkeypatch, capsys):
    admin_factory = Mock()
    monkeypatch.setattr(module, "AdminClient", admin_factory)
    monkeypatch.setattr(sys, "argv", ["script", "--run-name", "test-run"])

    module.main()

    admin_factory.assert_not_called()
    output = capsys.readouterr().out
    plan_text, marker = output.split("\nPREVIEW ONLY:", maxsplit=1)
    assert json.loads(plan_text)["topics"] == module.topic_names("test-run")
    assert "no Kafka connection or writes." in marker


def configure_create(monkeypatch, existing_topics):
    admin = Mock()
    admin.list_topics.return_value = Mock(topics=existing_topics)
    monkeypatch.setattr(module, "AdminClient", Mock(return_value=admin))
    monkeypatch.setattr(
        sys,
        "argv",
        ["script", "--run-name", "test-run", "--create"],
    )
    return admin


def test_existing_topic_blocks_creation(monkeypatch):
    name = module.topic_names("test-run")["features"]
    admin = configure_create(monkeypatch, {name: Mock()})

    with pytest.raises(SystemExit, match="Refusing to reuse existing topics"):
        module.main()

    admin.create_topics.assert_not_called()


def test_creates_all_topics_with_policy(monkeypatch, capsys):
    admin = configure_create(monkeypatch, {})
    futures = {name: Mock() for name in module.topic_names("test-run").values()}
    admin.create_topics.return_value = futures

    module.main()

    requests = admin.create_topics.call_args.args[0]
    assert {request.topic for request in requests} == set(futures)
    for request in requests:
        assert request.num_partitions == 1
        assert request.replication_factor == 1
        assert request.config == module.TOPIC_CONFIG
    for future in futures.values():
        future.result.assert_called_once_with()
    assert "PASS: all five replay topics created" in capsys.readouterr().out


def test_reports_partial_creation_failure(monkeypatch, capsys):
    admin = configure_create(monkeypatch, {})
    futures = {name: Mock() for name in module.topic_names("test-run").values()}
    failed_name = module.topic_names("test-run")["late"]
    futures[failed_name].result.side_effect = KafkaException(
        KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)
    )
    admin.create_topics.return_value = futures

    with pytest.raises(SystemExit, match="not fully successful"):
        module.main()

    output = capsys.readouterr().out
    assert failed_name in output
    assert '"errors": {' in output
    assert "PASS: all five replay topics created" not in output
    for future in futures.values():
        future.result.assert_called_once_with()
