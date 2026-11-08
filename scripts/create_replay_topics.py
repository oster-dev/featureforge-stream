"""Create isolated Kafka topics for a historical replay."""

import argparse
import json
import re

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC_CONFIG = {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",
    "retention.bytes": "-1",
    "message.timestamp.type": "LogAppendTime",
}


def topic_names(run_name: str) -> dict[str, str]:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", run_name):
        raise ValueError(
            "run-name must contain 1-80 lowercase letters, digits, or hyphens "
            "and start with a letter or digit"
        )

    base = f"interaction-events.replay.{run_name}"
    return {
        "input": f"{base}.v1",
        "validated": f"{base}.validated.v1",
        "rejected": f"{base}.rejected.v1",
        "features": f"features.replay.{run_name}.user-play-count-1h.v1",
        "late": f"{base}.late.v1",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview or create isolated historical replay topics."
    )
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--create", action="store_true")
    arguments = parser.parse_args()

    try:
        topics = topic_names(arguments.run_name)
    except ValueError as exc:
        parser.error(str(exc))

    plan = {
        "run_name": arguments.run_name,
        "bootstrap_servers": arguments.bootstrap_servers,
        "partitions": 1,
        "replication_factor": 1,
        "topic_config": TOPIC_CONFIG,
        "topics": topics,
    }

    print(json.dumps(plan, indent=2), flush=True)

    if not arguments.create:
        print("PREVIEW ONLY: no Kafka connection or writes.")
        return

    admin = AdminClient(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "broker.address.family": "v4",
            "client.id": "featureforge-stream-replay-topic-setup",
        }
    )

    metadata = admin.list_topics(timeout=10)
    existing = sorted(set(topics.values()) & set(metadata.topics))
    if existing:
        raise SystemExit(
            f"Refusing to reuse existing topics: {existing}. "
            "Choose a new run-name; no topics were created by this invocation."
        )

    requests = [
        NewTopic(
            name,
            num_partitions=1,
            replication_factor=1,
            config=dict(TOPIC_CONFIG),
        )
        for name in topics.values()
    ]

    futures = admin.create_topics(requests, request_timeout=30)
    created = []
    errors = {}

    for name, future in futures.items():
        try:
            future.result()
            created.append(name)
        except KafkaException as exc:
            errors[name] = str(exc)

    print(
        json.dumps(
            {
                "created_topics": sorted(created),
                "errors": errors,
            },
            indent=2,
        )
    )

    if errors:
        raise SystemExit(
            "Topic creation was not fully successful. Some topics may exist. "
            "Do not publish input until the failure has been reviewed."
        )

    print("PASS: all five replay topics created")


if __name__ == "__main__":
    main()
