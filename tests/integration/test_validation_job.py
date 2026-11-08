"""Verify validation routing using records unique to each test run."""

import json
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import pytest

from featureforge_stream.contracts.events import InteractionEventV1

FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "user_play_count_1h_v1.jsonl"


BOOTSTRAP_SERVERS = "kafka:29092"
INPUT_TOPIC = "interaction-events.v1"
VALIDATED_TOPIC = "interaction-events.validated.v1"
REJECTED_TOPIC = "interaction-events.rejected.v1"


def run_kafka(
    script: str,
    *arguments: str,
    input_text: str | None = None,
) -> str:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            f"/opt/kafka/bin/{script}",
            *arguments,
        ],
        input=input_text,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
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


def produce_records(*records: str) -> None:
    run_kafka(
        "kafka-console-producer.sh",
        "--bootstrap-server",
        BOOTSTRAP_SERVERS,
        "--topic",
        INPUT_TOPIC,
        input_text="\n".join(records) + "\n",
    )


def read_records(topic: str) -> list[dict]:
    output = run_kafka(
        "kafka-console-consumer.sh",
        "--bootstrap-server",
        BOOTSTRAP_SERVERS,
        "--topic",
        topic,
        "--from-beginning",
        "--timeout-ms",
        "3000",
    )

    return [json.loads(line) for line in output.splitlines() if line.strip()]


def wait_for_record(
    topic: str,
    predicate: Callable[[dict], bool],
    *,
    timeout_seconds: int = 45,
) -> dict:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        for record in read_records(topic):
            if predicate(record):
                return record

        time.sleep(0.5)

    raise AssertionError(
        f"No matching record found in {topic!r} "
        f"within the polling period of {timeout_seconds} seconds"
    )


@pytest.mark.integration
def test_valid_and_invalid_records_are_written_to_separate_topics() -> None:
    for topic in (INPUT_TOPIC, VALIDATED_TOPIC, REJECTED_TOPIC):
        create_topic(topic)

    fixture_payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8").splitlines()[0])

    first_event = InteractionEventV1.model_validate({**fixture_payload, "event_id": str(uuid4())})
    second_event = InteractionEventV1.model_validate({**fixture_payload, "event_id": str(uuid4())})

    run_id = str(uuid4())
    invalid_payload = f'{{"integration_run":"{run_id}","broken":'

    produce_records(
        first_event.model_dump_json(),
        invalid_payload,
        second_event.model_dump_json(),
    )

    first_result = wait_for_record(
        VALIDATED_TOPIC,
        lambda record: record.get("event_id") == str(first_event.event_id),
    )
    rejection = wait_for_record(
        REJECTED_TOPIC,
        lambda record: record.get("raw_payload") == invalid_payload,
    )
    second_result = wait_for_record(
        VALIDATED_TOPIC,
        lambda record: record.get("event_id") == str(second_event.event_id),
    )

    assert InteractionEventV1.model_validate(first_result) == first_event
    assert InteractionEventV1.model_validate(second_result) == second_event

    assert rejection["error_schema_version"] == "v1"
    assert rejection["reason_code"] == "INVALID_JSON"
    assert rejection["raw_payload"] == invalid_payload
    assert rejection["errors"]
    assert any(error["type"] == "json_invalid" for error in rejection["errors"])
