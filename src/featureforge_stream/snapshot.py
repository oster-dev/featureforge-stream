"""Parquet artifacts for bounded user-play-count streaming output.

These snapshots are not FeatureForge's canonical batch feature tables.
They represent event-time reconstruction, not historical online availability.
"""

from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from featureforge_stream.online_store import UserPlayCountFeatureV1

SNAPSHOT_SCHEMA = pa.schema(
    [
        pa.field("feature_schema_version", pa.string(), nullable=False),
        pa.field("feature_name", pa.string(), nullable=False),
        pa.field("user_id", pa.string(), nullable=False),
        pa.field("play_count", pa.int64(), nullable=False),
        pa.field("observation_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("window_start_exclusive", pa.timestamp("us", tz="UTC"), nullable=False),
    ],
    metadata={
        b"snapshot_contract": b"featureforge-stream.user-play-count-1h.snapshot.v1",
        b"window_interval": b"(window_start_exclusive, observation_time]",
        b"same_window_policy": b"maximum_play_count_append_only",
        b"availability_semantics": b"event_time_reconstruction_not_online_availability",
    },
)


def consolidate_windows(
    features: Iterable[UserPlayCountFeatureV1],
) -> list[UserPlayCountFeatureV1]:
    """Keep the highest count per user/window, sorted by user and observation."""
    windows: dict[tuple[str, datetime], UserPlayCountFeatureV1] = {}

    for feature in features:
        if not isinstance(feature, UserPlayCountFeatureV1):
            raise TypeError("features must contain UserPlayCountFeatureV1 instances")

        key = (feature.user_id, feature.observation_time)
        previous = windows.get(key)
        if previous is None or feature.play_count > previous.play_count:
            windows[key] = feature

    return [windows[key] for key in sorted(windows)]


def write_snapshot(
    features: Iterable[UserPlayCountFeatureV1],
    *,
    output_path: str | Path,
) -> int:
    """Write a new, non-empty snapshot and return its consolidated row count.

    This bounded local implementation consolidates and serializes in memory.
    Existing destinations are never overwritten. Filesystem write failures
    can leave a partial destination; publication is not crash-atomic.
    """
    consolidated = consolidate_windows(features)
    if not consolidated:
        raise ValueError("Refusing to write an empty snapshot")

    table = pa.Table.from_pylist(
        [feature.model_dump(mode="python") for feature in consolidated],
        schema=SNAPSHOT_SCHEMA,
    )

    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer, compression="snappy")
    payload = buffer.getvalue().to_pybytes()

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as target:
        target.write(payload)

    return len(consolidated)


def read_snapshot(
    input_path: str | Path,
) -> list[UserPlayCountFeatureV1]:
    """Read and validate the exact snapshot schema and consolidated row order."""
    table = pq.ParquetFile(input_path).read()

    if not table.schema.equals(SNAPSHOT_SCHEMA, check_metadata=True):
        raise ValueError("Unsupported snapshot schema or contract metadata")
    if table.num_rows == 0:
        raise ValueError("Refusing to read an empty snapshot")
    if any(column.null_count for column in table.columns):
        raise ValueError("Snapshot contains null values")

    features = [UserPlayCountFeatureV1.model_validate(row) for row in table.to_pylist()]

    if features != consolidate_windows(features):
        raise ValueError("Snapshot rows must be unique and sorted by user/window")

    return features
