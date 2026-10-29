"""Tests that sink restarts resume from DuckDB offsets instead of Kafka committed offsets."""

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


def test_sink_resumes_from_duckdb_offsets(temp_lake):
    lake_dir, state_db = temp_lake

    # Seed DuckDB with prior partition offsets
    con = duckdb.connect(state_db)
    con.execute("""
        CREATE TABLE IF NOT EXISTS partition_offsets (
            topic VARCHAR,
            partition INTEGER,
            max_committed_offset BIGINT,
            PRIMARY KEY (topic, partition)
        );
    """)
    con.execute("""
        INSERT INTO partition_offsets VALUES
        ('trips', 0, 1500),
        ('trips', 1, 2800),
        ('trips', 2, 4200);
    """)
    con.close()

    # Restarting the sink should seek to max_committed_offset + 1
    sink = SinkConsumer(
        mode="effectively-once",
        batch_size=500,
        lake_dir=lake_dir,
        state_db_path=state_db,
        force_local=True,
    )

    assert sink.consumer.current_offsets[0] == 1501
    assert sink.consumer.current_offsets[1] == 2801
    assert sink.consumer.current_offsets[2] == 4201
    assert sink.consumer.current_offsets[3] == 0

    sink.consumer.close()
    if sink.con:
        sink.con.close()
