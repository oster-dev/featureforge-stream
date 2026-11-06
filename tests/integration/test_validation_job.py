"""The validation job separates valid and rejected Kafka records."""

import json
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from featureforge_stream.contracts.events import InteractionEventV1

FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "user_play_count_1h_v1.jsonl"

BOOTSTRAP_SERVERS = "kafka:29092"
INPUT_TOPIC = "interaction-events.v1"
VALIDATED_TOPIC = "interaction-events.validated.v1"
REJECTED_TOPIC = "interaction-events.rejected.v1"


def run_kafka(script: str, *arguments: str) -> str:
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        f"/opt/kafka/bin/{script}",
        *arguments,
    ]

    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def create_topic(topic: str) -> None:
    run_kafka(
        "kafka-topics.sh",
        "--bootstrap-server",
        BOOTSTRAP_SERVERS,
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    )


def read_records(topic: str) -> list[str]:
    output = run_kafka(
        "kafka-console-consumer.sh",
        "--bootstrap-server",
        BOOTSTRAP_SERVERS,
        "--topic",
        topic,
        "--from-beginning",
        "--timeout-ms",
        "5000",
    )

    return [line for line in output.splitlines() if line.strip()]


def wait_for_record(
    topic: str,
    predicate,
    timeout_seconds: int = 30,
) -> str:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        for record in read_records(topic):
            if predicate(record):
                return record

        time.sleep(1)

    raise AssertionError(f"No matching record found in topic {topic!r}")


def produce_records(*records: str) -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server",
            BOOTSTRAP_SERVERS,
            "--topic",
            INPUT_TOPIC,
        ],
        input="\n".join(records) + "\n",
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.integration
def test_valid_and_invalid_records_are_written_to_separate_topics() -> None:
    create_topic(INPUT_TOPIC)
    create_topic(VALIDATED_TOPIC)
    create_topic(REJECTED_TOPIC)

    fixture_event = json.loads(FIXTURE_PATH.read_text(encoding="utf-8").splitlines()[0])

    first_event_id = str(uuid.uuid4())
    first_event = {
        **fixture_event,
        "event_id": first_event_id,
        "user_id": f"integration-user-{first_event_id[:8]}",
    }
    invalid_payload = '{"broken":'

    produce_records(json.dumps(first_event), invalid_payload)

    validated_record = wait_for_record(
        VALIDATED_TOPIC,
        lambda record: first_event_id in record,
    )
    rejected_record = wait_for_record(
        REJECTED_TOPIC,
        lambda record: (
            record.startswith("{") and json.loads(record).get("raw_payload") == invalid_payload
        ),
    )

    InteractionEventV1.model_validate_json(validated_record)
    assert json.loads(validated_record)["event_id"] == first_event_id

    rejection = json.loads(rejected_record)
    assert rejection["reason_code"] == "INVALID_JSON"
    assert rejection["raw_payload"] == invalid_payload

    second_event_id = str(uuid.uuid4())
    second_event = {
        **fixture_event,
        "event_id": second_event_id,
        "user_id": f"integration-user-{second_event_id[:8]}",
    }

    produce_records(json.dumps(second_event))

    second_validated_record = wait_for_record(
        VALIDATED_TOPIC,
        lambda record: second_event_id in record,
    )

    InteractionEventV1.model_validate_json(second_validated_record)
    assert json.loads(second_validated_record)["event_id"] == second_event_id
