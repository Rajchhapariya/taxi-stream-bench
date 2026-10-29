"""End-to-end integration test on a 10,000-row sample of NYC Yellow Taxi data."""

import os
import shutil
import duckdb
import pytest
from pipeline.producer import run_producer
from pipeline.sink import SinkConsumer
from pipeline.queries import query_hourly_trips, query_avg_fare_per_distance, query_top_10_pairs


@pytest.fixture
def integration_env(tmp_path):
    lake_dir = tmp_path / "lake"
    state_db = lake_dir / "sink_state.duckdb"
    broker_state = tmp_path / ".broker_state"
    lake_dir.mkdir(parents=True, exist_ok=True)
    broker_state.mkdir(parents=True, exist_ok=True)

    yield {
        "lake_dir": str(lake_dir),
        "state_db": str(state_db),
        "broker_state": str(broker_state),
    }

    if os.path.exists(str(lake_dir)):
        shutil.rmtree(str(lake_dir), ignore_errors=True)
    if os.path.exists(str(broker_state)):
        shutil.rmtree(str(broker_state), ignore_errors=True)


def test_pipeline_integration_10000_sample(integration_env):
    lake_dir = integration_env["lake_dir"]
    state_db = integration_env["state_db"]
    broker_state = integration_env["broker_state"]

    source_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        "yellow_tripdata_2024-01.parquet",
    )
    if not os.path.exists(source_path):
        pytest.skip(f"Source parquet file not found at {source_path}")

    # Produce 10k rows
    os.environ["LOCAL_BROKER_DIR"] = broker_state
    prod_summary = run_producer(
        source_path=source_path,
        topic="trips_integration",
        max_records=10000,
        force_local=True,
        save_stats=None,
    )
    produced_count = prod_summary["accepted_and_produced"]
    assert produced_count == 10000

    # Ingest through Mode B sink
    sink = SinkConsumer(
        mode="effectively-once",
        batch_size=2000,
        batch_timeout=0.5,
        lake_dir=lake_dir,
        state_db_path=state_db,
        topic="trips_integration",
        force_local=True,
    )
    sink.run(max_messages=10000, max_seconds=15.0)

    # Check lake row count and duplicates
    con = duckdb.connect()
    norm_lake = lake_dir.replace("\\", "/")
    parquet_path = f"{norm_lake}/*/*.parquet"

    total_in_lake, unique_in_lake, duplicates = con.execute(f"""
        SELECT 
            count(*) as total_rows,
            count(distinct row_id) as unique_rows,
            count(*) - count(distinct row_id) as duplicate_rows
        FROM read_parquet('{parquet_path}')
    """).fetchone()

    assert total_in_lake == 10000
    assert unique_in_lake == 10000
    assert duplicates == 0

    # Test the three analytical queries over the written files
    q1_res, q1_dur = query_hourly_trips(lake_dir, con)
    q2_res, q2_dur = query_avg_fare_per_distance(lake_dir, con)
    q3_res, q3_dur = query_top_10_pairs(lake_dir, con)

    assert len(q1_res) > 0
    assert len(q2_res) > 0
    assert len(q3_res) == 10
    con.close()
