"""Tests that a crash before atomic rename leaves only temp files and no partial Parquet output."""

import glob
import os
import shutil
import duckdb
import pytest
from pipeline.sink import SinkConsumer, clean_orphan_tmp_files


@pytest.fixture
def temp_lake(tmp_path):
    lake_dir = tmp_path / "lake"
    state_db = lake_dir / "sink_state.duckdb"
    lake_dir.mkdir(parents=True, exist_ok=True)
    yield str(lake_dir), str(state_db)
    if os.path.exists(str(lake_dir)):
        shutil.rmtree(str(lake_dir), ignore_errors=True)


def test_crash_before_rename_leaves_no_visible_partial_file(temp_lake):
    lake_dir, state_db = temp_lake

    sink = SinkConsumer(
        mode="effectively-once",
        batch_size=50,
        batch_timeout=10.0,
        lake_dir=lake_dir,
        state_db_path=state_db,
        crash_before_rename=True,
        force_local=True,
    )

    sample_records = [
        {
            "row_id": i,
            "produce_ts": "2024-01-15T12:00:00Z",
            "produce_epoch_ms": 1705320000000,
            "VendorID": 2,
            "tpep_pickup_datetime": "2024-01-15 12:00:00",
            "tpep_dropoff_datetime": "2024-01-15 12:15:00",
            "passenger_count": 2,
            "trip_distance": 1.8,
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": 142,
            "DOLocationID": 236,
            "payment_type": 1,
            "fare_amount": 9.5,
            "extra": 0.0,
            "mta_tax": 0.5,
            "tip_amount": 2.0,
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": 13.0,
            "congestion_surcharge": 2.5,
            "Airport_fee": 0.0,
        }
        for i in range(50)
    ]

    sink.buffers[1] = list(sample_records)
    sink.start_offsets[1] = 0
    sink.end_offsets[1] = 49

    with pytest.raises(SystemExit) as excinfo:
        sink._flush_partition(1)

    assert excinfo.value.code == 43

    # Lake should not contain any final .parquet files
    parquet_files = glob.glob(os.path.join(lake_dir, "**", "*.parquet"), recursive=True)
    assert len(parquet_files) == 0

    # Only an uncommitted .tmp file should exist
    tmp_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(lake_dir)
        for f in files if f.endswith(".tmp")
    ]
    assert len(tmp_files) == 1

    # Cleanup removes the orphan .tmp file
    clean_orphan_tmp_files(lake_dir)

    tmp_files_after = [
        os.path.join(root, f)
        for root, _, files in os.walk(lake_dir)
        for f in files if f.endswith(".tmp")
    ]
    assert len(tmp_files_after) == 0
