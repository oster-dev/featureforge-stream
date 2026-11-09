"""Verify real skip-rate Flink output through Kafka materialization into Redis."""

import argparse
import json
import os
import re
import subprocess
import time
from datetime import UTC, datetime, timedelta
from urllib.request import urlopen
from uuid import uuid4

import pytest

from featureforge_stream.contracts.events import InteractionEventV1
from featureforge_stream.reference.user_skip_rate import calculate_user_skip_rate_24h


def run_command(*arguments: str) -> str:
    result = subprocess.run(
        arguments,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.stdout + result.stderr


def flink_api(path: str) -> dict:
    with urlopen(f"http://127.0.0.1:8081/{path}", timeout=10) as response:
        return json.load(response)


@pytest.mark.integration
def test_skip_rate_flink_to_redis_corrections_and_replay() -> None:
    from confluent_kafka import Consumer, Producer, TopicPartition
    from redis import Redis

    from featureforge_stream.materialize_skip_rate import materialize
    from featureforge_stream.skip_rate_store import RedisUserSkipRateStore

    overview = flink_api("overview")
    assert overview["slots-available"] >= 1, "This test requires at least one free Flink task slot"

    namespace = uuid4().hex
    prefix = f"test.skip-rate-online.{namespace}"
    input_topic = f"{prefix}.input"
    output_topic = f"{prefix}.features"
    late_topic = f"{prefix}.late"
    user_id = f"skip_rate_{namespace}"
    redis_url = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
    redis_prefix = f"featureforge-stream:integration:flink-to-redis:{namespace}"

    for topic in (input_topic, output_topic, late_topic):
        run_command(
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:29092",
            "--create",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        )

    observation = datetime.now(UTC).replace(second=0, microsecond=0)
    progress_time = observation + timedelta(minutes=11)

    def event(kind: str, *, seconds_before: int, user: str = user_id):
        return InteractionEventV1.model_validate(
            {
                "event_id": uuid4(),
                "event_time": observation - timedelta(seconds=seconds_before),
                "ingested_at": progress_time + timedelta(seconds=1),
                "user_id": user,
                "content_id": "content_skip_rate_test",
                "event_type": kind,
                "play_seconds": 0,
                "source": "web",
                "schema_version": "v1",
            }
        )

    initial_play = event("play", seconds_before=120)
    initial_skip = event("skip", seconds_before=90)
    late_play = event("play", seconds_before=60)
    late_skip = event("skip", seconds_before=30)
    probe_play = event("play", seconds_before=15)
    progress = event(
        "impression",
        seconds_before=-660,
        user=f"progress_{namespace}",
    )

    producer = Producer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "broker.address.family": "v4",
            "enable.idempotence": True,
            "message.timeout.ms": 30000,
        }
    )
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "broker.address.family": "v4",
            "group.id": prefix,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )

    def produce(events):
        errors = []

        def delivered(error, message):
            if error is not None:
                errors.append(str(error))

        for item in events:
            producer.produce(
                input_topic,
                value=item.model_dump_json(),
                on_delivery=delivered,
            )
        assert producer.flush(35) == 0, "Producer delivery timed out"
        assert not errors, errors

    def next_target(timeout: float):
        message = consumer.poll(timeout)
        if message is None:
            return None
        assert message.error() is None, str(message.error())
        record = json.loads(message.value())
        if record.get("user_id") != user_id:
            return None
        if datetime.fromisoformat(record["observation_time"]) != observation:
            return None

        assert record["feature_schema_version"] == "v1"
        assert record["feature_name"] == "user_skip_rate_24h"
        assert datetime.fromisoformat(record["window_start_exclusive"]) == observation - timedelta(
            hours=24
        )

        total = record["play_count"] + record["skip_count"]
        expected_rate = record["skip_count"] / total if total else 0.0
        assert record["skip_rate"] == pytest.approx(expected_rate)
        return record

    materializer_arguments = argparse.Namespace(
        bootstrap_servers="127.0.0.1:9092",
        redis_url=redis_url,
        topic=output_topic,
        group_id=f"{prefix}.materializer",
        key_prefix=redis_prefix,
        commit_every=1000,
        bounded=True,
    )

    def verify_online(plays: int, skips: int, logical_events):
        deadline = time.monotonic() + 90
        latest = None

        with Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        ) as client:
            assert client.ping() is True
            store = RedisUserSkipRateStore(client, key_prefix=redis_prefix)

            while time.monotonic() < deadline:
                materialize(materializer_arguments)
                latest = store.read(user_id)

                if latest is not None:
                    assert latest.observation_time >= observation
                    assert latest.play_count <= plays, latest
                    assert latest.skip_count <= skips, latest

                    if (latest.play_count, latest.skip_count) == (plays, skips):
                        expected_rate = calculate_user_skip_rate_24h(
                            logical_events,
                            user_id=user_id,
                            observation_time=latest.observation_time,
                        )
                        assert latest.skip_rate == pytest.approx(expected_rate)
                        assert latest.window_start_exclusive == (
                            latest.observation_time - timedelta(hours=24)
                        )
                        return latest

                time.sleep(0.5)

        raise AssertionError(f"Redis did not reach plays={plays}, skips={skips}; latest={latest}")

    def wait_for_counts(plays: int, skips: int, logical_events):
        expected_rate = calculate_user_skip_rate_24h(
            logical_events,
            user_id=user_id,
            observation_time=observation,
        )
        deadline = time.monotonic() + 90
        last = None
        while time.monotonic() < deadline:
            record = next_target(1.0)
            if record is None:
                continue
            last = record
            assert record["play_count"] <= plays, record
            assert record["skip_count"] <= skips, record
            if (record["play_count"], record["skip_count"]) == (plays, skips):
                assert record["skip_rate"] == pytest.approx(expected_rate)
                online = verify_online(plays, skips, logical_events)
                print(
                    f"Verified Flink -> Kafka -> Redis: "
                    f"plays={online.play_count}, skips={online.skip_count}, "
                    f"rate={online.skip_rate}, "
                    f"observation_time={online.observation_time.isoformat()}"
                )
                return record
        raise AssertionError(f"Expected plays={plays}, skips={skips}; last target output={last}")

    job_id = None
    try:
        consumer.assign([TopicPartition(output_topic, 0, 0)])

        submission = run_command(
            "docker",
            "compose",
            "exec",
            "-T",
            "jobmanager",
            "/opt/flink/bin/flink",
            "run",
            "--detached",
            "--python",
            "/opt/flink/jobs/user_skip_rate_24h.py",
            "--input-topic",
            input_topic,
            "--feature-topic",
            output_topic,
            "--late-topic",
            late_topic,
            "--group-id",
            prefix,
            "--job-name",
            prefix,
        )
        print(submission)
        match = re.search(r"JobID\s+([0-9a-fA-F]{32})", submission)
        assert match is not None, submission
        job_id = match.group(1)

        deadline = time.monotonic() + 90
        while True:
            job = flink_api(f"jobs/{job_id}")
            assert job["state"] not in {"FAILED", "CANCELED", "FINISHED"}, job
            vertices = job.get("vertices", [])
            if (
                job["state"] == "RUNNING"
                and vertices
                and all(vertex["status"] == "RUNNING" for vertex in vertices)
            ):
                break
            assert time.monotonic() < deadline, job
            time.sleep(1)

        logical_events = [initial_play, initial_skip]
        produce([*logical_events, progress])
        wait_for_counts(1, 1, logical_events)

        logical_events.append(late_play)
        produce([late_play])
        wait_for_counts(2, 1, logical_events)

        logical_events.append(late_skip)
        produce([late_skip])
        wait_for_counts(2, 2, logical_events)

        logical_events.append(probe_play)
        produce([initial_play, initial_skip, late_play, late_skip, probe_play])
        latest = wait_for_counts(3, 2, logical_events)

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            record = next_target(0.5)
            if record is not None:
                latest = record
                assert (record["play_count"], record["skip_count"]) == (3, 2)
                assert record["skip_rate"] == pytest.approx(0.4)

        assert (latest["play_count"], latest["skip_count"]) == (3, 2)
        online_before = verify_online(3, 2, logical_events)

        materializer_arguments.group_id = f"{prefix}.materializer-replay"
        online_after = verify_online(3, 2, logical_events)
        assert online_after == online_before
        print("Verified replay of actual Flink output preserves Redis value")

        assert flink_api(f"jobs/{job_id}")["state"] == "RUNNING"
    finally:
        consumer.close()
        try:
            with Redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=3,
            ) as client:
                keys = list(client.scan_iter(match=f"{redis_prefix}:*"))
                if keys:
                    client.delete(*keys)
        finally:
            if job_id is not None:
                print(
                    run_command(
                        "docker",
                        "compose",
                        "exec",
                        "-T",
                        "jobmanager",
                        "/opt/flink/bin/flink",
                        "cancel",
                        job_id,
                    )
                )
