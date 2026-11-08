"""Test stream snapshot consolidation, validation, and Parquet round trips."""

from datetime import UTC, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from featureforge_stream.online_store import UserPlayCountFeatureV1
from featureforge_stream.snapshot import (
    SNAPSHOT_SCHEMA,
    consolidate_windows,
    read_snapshot,
    write_snapshot,
)


def make_feature(*, user_id="user_1", count=2, minute=0):
    observation = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
    observation += timedelta(minutes=minute)
    return UserPlayCountFeatureV1(
        user_id=user_id,
        play_count=count,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=1),
    )


def write_raw(path, features):
    table = pa.Table.from_pylist(
        [feature.model_dump(mode="python") for feature in features],
        schema=SNAPSHOT_SCHEMA,
    )
    pq.write_table(table, path)


def test_same_window_keeps_highest_count():
    features = [make_feature(count=count) for count in (2, 5, 1, 5)]

    assert consolidate_windows(features) == [make_feature(count=5)]


def test_newer_window_preserves_older_window_and_zero_count():
    older = make_feature(count=5)
    newer = make_feature(count=0, minute=1)

    assert consolidate_windows([newer, older]) == [older, newer]


def test_users_are_separate_and_sorted():
    first = make_feature(user_id="user_1")
    second = make_feature(user_id="user_2")

    assert consolidate_windows([second, first]) == [first, second]


def test_order_and_duplicate_deliveries_do_not_change_result():
    features = [
        make_feature(count=1),
        make_feature(count=4),
        make_feature(count=0, minute=1),
        make_feature(user_id="user_2"),
    ]
    expected = consolidate_windows(features)

    assert consolidate_windows(reversed(features)) == expected
    assert consolidate_windows(features + features) == expected


def test_equivalent_timezone_offsets_identify_same_window():
    original = make_feature(count=2)
    observation = original.observation_time.astimezone(timezone(timedelta(hours=2)))
    correction = UserPlayCountFeatureV1(
        user_id=original.user_id,
        play_count=5,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=1),
    )

    result = consolidate_windows([original, correction])

    assert len(result) == 1
    assert result[0].play_count == 5
    assert result[0].observation_time == original.observation_time


@pytest.mark.parametrize("invalid", [None, {}, "payload"])
def test_unvalidated_items_are_rejected(invalid):
    with pytest.raises(TypeError, match="UserPlayCountFeatureV1"):
        consolidate_windows([invalid])


def test_parquet_round_trip_preserves_fields_precision_and_schema(tmp_path):
    features = [
        make_feature(count=1),
        make_feature(count=4),
        make_feature(count=0, minute=1),
        make_feature(user_id="user_2"),
    ]
    output = tmp_path / "snapshot.parquet"

    rows = write_snapshot(iter(features), output_path=output)
    restored = read_snapshot(output)
    table = pq.ParquetFile(output).read()

    assert rows == 3
    assert restored == consolidate_windows(features)
    assert restored[0].observation_time.microsecond == 123456
    assert table.schema.equals(SNAPSHOT_SCHEMA, check_metadata=True)
    assert all(column.null_count == 0 for column in table.columns)


def test_empty_write_is_rejected_without_creating_file(tmp_path):
    output = tmp_path / "snapshot.parquet"

    with pytest.raises(ValueError, match="empty snapshot"):
        write_snapshot([], output_path=output)

    assert not output.exists()


def test_existing_destination_is_not_overwritten(tmp_path):
    output = tmp_path / "snapshot.parquet"
    output.write_bytes(b"existing artifact")

    with pytest.raises(FileExistsError):
        write_snapshot([make_feature()], output_path=output)

    assert output.read_bytes() == b"existing artifact"


def test_parent_directory_is_created(tmp_path):
    output = tmp_path / "run_1" / "snapshot.parquet"

    assert write_snapshot([make_feature()], output_path=output) == 1
    assert read_snapshot(output) == [make_feature()]


def test_invalid_input_does_not_create_destination(tmp_path):
    output = tmp_path / "snapshot.parquet"

    with pytest.raises(TypeError):
        write_snapshot([make_feature(), {}], output_path=output)

    assert not output.exists()


def test_read_rejects_empty_snapshot(tmp_path):
    output = tmp_path / "snapshot.parquet"
    write_raw(output, [])

    with pytest.raises(ValueError, match="empty snapshot"):
        read_snapshot(output)


def test_read_rejects_missing_contract_metadata(tmp_path):
    output = tmp_path / "snapshot.parquet"
    table = pa.Table.from_pylist(
        [make_feature().model_dump(mode="python")],
        schema=SNAPSHOT_SCHEMA,
    ).replace_schema_metadata(None)
    pq.write_table(table, output)

    with pytest.raises(ValueError, match="schema or contract"):
        read_snapshot(output)


def test_read_rejects_duplicate_window_rows(tmp_path):
    output = tmp_path / "snapshot.parquet"
    write_raw(output, [make_feature(count=1), make_feature(count=3)])

    with pytest.raises(ValueError, match="unique and sorted"):
        read_snapshot(output)


def test_read_rejects_unsorted_rows(tmp_path):
    output = tmp_path / "snapshot.parquet"
    write_raw(output, [make_feature(minute=1), make_feature()])

    with pytest.raises(ValueError, match="unique and sorted"):
        read_snapshot(output)


def test_read_rejects_invalid_feature_window(tmp_path):
    output = tmp_path / "snapshot.parquet"
    row = make_feature().model_dump(mode="python")
    row["window_start_exclusive"] = row["observation_time"]
    pq.write_table(
        pa.Table.from_pylist([row], schema=SNAPSHOT_SCHEMA),
        output,
    )

    with pytest.raises(ValueError, match="exactly one hour"):
        read_snapshot(output)
