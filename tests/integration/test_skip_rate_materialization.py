"""Verify real Kafka-to-Redis skip-rate materialization and offset resume."""

import argparse
import os
import subprocess
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from redis import Redis

from featureforge_stream.contracts.user_skip_rate import UserSkipRateFeatureV1
from featureforge_stream.skip_rate_store import RedisUserSkipRateStore

pytestmark = pytest.mark.integration


def test_kafka_to_redis_corrections_resume_and_replay() -> None:
    from confluent_kafka import Consumer, Producer, TopicPartition

    from featureforge_stream.materialize_skip_rate import materialize

    namespace = uuid4().hex
    topic = f"test.skip-rate-materialization.{namespace}"
    prefix = f"featureforge-stream:integration:skip-rate-materialization:{namespace}"
    group_id = f"skip-rate-materialization-{namespace}"
    redis_url = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
    observation = datetime(2026, 1, 2, 12, tzinfo=UTC)

    subprocess.run(
        [
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
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=45,
    )

    def feature(plays, skips, *, minutes=0):
        timestamp = observation + timedelta(minutes=minutes)
        total = plays + skips
        return UserSkipRateFeatureV1(
            user_id=f"user_{namespace}",
            play_count=plays,
            skip_count=skips,
            skip_rate=skips / total if total else 0.0,
            observation_time=timestamp,
            window_start_exclusive=timestamp - timedelta(hours=24),
        )

    arguments = argparse.Namespace(
        bootstrap_servers="127.0.0.1:9092",
        redis_url=redis_url,
        topic=topic,
        group_id=group_id,
        key_prefix=prefix,
        commit_every=1,
        bounded=True,
    )
    producer = Producer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "broker.address.family": "v4",
            "enable.idempotence": True,
            "message.timeout.ms": 30000,
        }
    )
    inspector = Consumer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "broker.address.family": "v4",
            "group.id": group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )

    def publish(value):
        errors = []

        def delivered(error, message):
            if error is not None:
                errors.append(str(error))

        producer.produce(
            topic,
            value=value.model_dump_json(),
            on_delivery=delivered,
        )
        assert producer.flush(35) == 0
        assert not errors, errors

    initial = feature(1, 1)
    downward = feature(2, 1)
    upward = feature(2, 2)
    newer_zero = feature(0, 0, minutes=1)

    phases = [
        (initial, "written", initial),
        (downward, "written", downward),
        (upward, "written", upward),
        (initial, "stale", upward),
        (upward, "unchanged", upward),
        (newer_zero, "written", newer_zero),
        (upward, "stale", newer_zero),
    ]

    try:
        with Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        ) as client:
            assert client.ping() is True
            store = RedisUserSkipRateStore(client, key_prefix=prefix)

            try:
                for next_offset, (incoming, outcome, expected) in enumerate(phases, start=1):
                    publish(incoming)
                    assert dict(materialize(arguments)) == {outcome: 1}
                    assert store.read(initial.user_id) == expected

                    committed = inspector.committed([TopicPartition(topic, 0)], timeout=10)[0]
                    assert committed.error is None
                    assert committed.offset == next_offset

                assert dict(materialize(arguments)) == {}
                assert store.read(initial.user_id) == newer_zero

                arguments.group_id = f"{group_id}-replay"
                assert dict(materialize(arguments)) == {
                    "stale": 6,
                    "unchanged": 1,
                }
                assert store.read(initial.user_id) == newer_zero
            finally:
                client.delete(store.key_for(initial.user_id))
    finally:
        inspector.close()
