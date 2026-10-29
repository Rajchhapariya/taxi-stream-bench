"""Tests that replaying an offset range produces exactly one Parquet file and no duplicates."""

import os
import shutil
import duckdb
import pytest
from pipeline.sink import SinkConsumer


@pytest.fixture
def temp_lake(tmp_path):
    lake_dir = tmp_path / "lake"
    state_db = lake_dir / "sink_state.duckdb"
    lake_dir.mkdir(parents=True, exist_ok=True)
    yield str(lake_dir), str(state_db)
    if os.path.exists(str(lake_dir)):
        shutil.rmtree(str(lake_dir), ignore_errors=True)


def test_replaying_same_offset_range_produces_no_duplicates(temp_lake):
    lake_dir, state_db = temp_lake

    sink = SinkConsumer(
        mode="effectively-once",
        batch_size=100,
        batch_timeout=10.0,
        lake_dir=lake_dir,
        state_db_path=state_db,
        force_local=True,
    )

    sample_records = [
        {
            "row_id": i,
            "produce_ts": "2024-01-15T10:00:00Z",
            "produce_epoch_ms": 1705312800000,
            "VendorID": 1,
            "tpep_pickup_datetime": "2024-01-15 10:00:00",
            "tpep_dropoff_datetime": "2024-01-15 10:20:00",
            "passenger_count": 1,
            "trip_distance": 2.5,
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": 161,
            "DOLocationID": 237,
            "payment_type": 1,
            "fare_amount": 12.0,
            "extra": 1.0,
            "mta_tax": 0.5,
            "tip_amount": 2.5,
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": 17.0,
            "congestion_surcharge": 2.5,
            "Airport_fee": 0.0,
        }
        for i in range(100)
    ]

    # First write: partition 0, offsets 0..99
    sink.buffers[0] = list(sample_records)
    sink.start_offsets[0] = 0
    sink.end_offsets[0] = 99
    sink._flush_partition(0)

    assert sink.total_rows_committed == 100
    assert sink.total_batches_committed == 1

    expected_file = os.path.join(lake_dir, "date=2024-01-15", "trips-p0-o0-o99.parquet")
    assert os.path.exists(expected_file)

    # Replay identical batch
    sink.buffers[0] = list(sample_records)
    sink.start_offsets[0] = 0
    sink.end_offsets[0] = 99
    sink._flush_partition(0)

    con = duckdb.connect(state_db)
    manifest_rows = con.execute("SELECT count(*) FROM lake_manifest WHERE partition = 0 AND start_offset = 0 AND end_offset = 99").fetchone()[0]
    assert manifest_rows == 1

    parquet_files = [f for f in os.listdir(os.path.join(lake_dir, "date=2024-01-15")) if f.endswith(".parquet")]
    assert len(parquet_files) == 1

    norm_lake = lake_dir.replace("\\", "/")
    duck_res = con.execute(f"""
        SELECT 
            count(*) as total_rows,
            count(distinct row_id) as unique_rows
        FROM read_parquet('{norm_lake}/*/*.parquet')
    """).fetchone()

    assert duck_res[0] == 100
    assert duck_res[1] == 100
    con.close()
