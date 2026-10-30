"""End-to-end integration tests running against a real Redpanda broker in Docker.

Marked with @pytest.mark.docker and skipped cleanly when Redpanda is not reachable.
"""

import os
import subprocess
import sys
import time
import uuid

import duckdb
import pytest

from bench.common import hard_kill_process, lake_row_ids, lake_stats
from pipeline.broker import (create_kafka_topic, delete_kafka_topic, kafka_usable)
from pipeline.producer import run_producer
from pipeline.queries import query_avg_fare_per_distance, query_hourly_trips, query_top_10_pairs
from pipeline.sink import SinkConsumer

pytestmark = pytest.mark.docker
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "yellow_tripdata_2024-01.parquet")


@pytest.fixture(autouse=True)
def check_redpanda_available():
    if not kafka_usable(BOOTSTRAP_SERVERS):
        pytest.skip(f"Redpanda broker is not reachable at {BOOTSTRAP_SERVERS}")
    if not os.path.exists(DATA_PATH):
        pytest.skip(f"Data file not found at {DATA_PATH}")


@pytest.fixture
def redpanda_topic():
    topic_name = f"trips_test_{uuid.uuid4().hex[:8]}"
    create_kafka_topic(topic_name, BOOTSTRAP_SERVERS, num_partitions=6)
    try:
        yield topic_name
    finally:
        delete_kafka_topic(topic_name, BOOTSTRAP_SERVERS)


def test_redpanda_mode_a_produces_and_sinks(redpanda_topic, tmp_path):
    lake_dir = str(tmp_path / "lake_mode_a")
    state_db = str(tmp_path / "lake_mode_a" / "sink_state.duckdb")

    prod_summary = run_producer(
        source_path=DATA_PATH,
        topic=redpanda_topic,
        max_records=10000,
        require_kafka=True,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        save_stats=None,
    )
    produced_count = prod_summary["accepted_and_produced"]
    assert produced_count == 10000

    sink = SinkConsumer(
        mode="at-least-once",
        batch_size=2000,
        batch_timeout=0.5,
        lake_dir=lake_dir,
        state_db_path=state_db,
        topic=redpanda_topic,
        require_kafka=True,
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )
    sink.run(max_messages=10000, max_seconds=30.0)

    stats = lake_stats(lake_dir)
    assert stats["rows"] == 10000
    assert stats["unique_row_ids"] == 10000


def test_redpanda_mode_b_and_analytical_queries(redpanda_topic, tmp_path):
    lake_dir = str(tmp_path / "lake_mode_b")
    state_db = str(tmp_path / "lake_mode_b" / "sink_state.duckdb")

    prod_summary = run_producer(
        source_path=DATA_PATH,
        topic=redpanda_topic,
        max_records=10000,
        require_kafka=True,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        return_row_ids=True,
        save_stats=None,
    )
    produced_count = prod_summary["accepted_and_produced"]
    produced_ids = prod_summary["produced_row_ids"]
    assert produced_count == 10000

    sink = SinkConsumer(
        mode="effectively-once",
        batch_size=2000,
        batch_timeout=0.5,
        lake_dir=lake_dir,
        state_db_path=state_db,
        topic=redpanda_topic,
        require_kafka=True,
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )
    sink.run(max_messages=10000, max_seconds=30.0)

    stats = lake_stats(lake_dir)
    lake_ids = lake_row_ids(lake_dir)
    duplicates = stats["rows"] - stats["unique_row_ids"]
    missing = len(produced_ids - lake_ids)

    assert stats["rows"] == 10000
    assert duplicates == 0
    assert missing == 0

    con = duckdb.connect()
    q1_res, _ = query_hourly_trips(lake_dir, con)
    q2_res, _ = query_avg_fare_per_distance(lake_dir, con)
    q3_res, _ = query_top_10_pairs(lake_dir, con)

    assert len(q1_res) > 0
    assert len(q2_res) > 0
    assert len(q3_res) == 10
    con.close()


def test_redpanda_mode_b_crash_injection(redpanda_topic, tmp_path):
    lake_dir = str(tmp_path / "lake_mode_b_crash")
    state_db = str(tmp_path / "lake_mode_b_crash" / "sink_state.duckdb")

    prod_summary = run_producer(
        source_path=DATA_PATH,
        topic=redpanda_topic,
        max_records=10000,
        require_kafka=True,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        return_row_ids=True,
        save_stats=None,
    )
    produced_count = prod_summary["accepted_and_produced"]
    produced_ids = prod_summary["produced_row_ids"]
    assert produced_count == 10000

    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR

    def launch_sink():
        cmd = [
            sys.executable, "-u", "-m", "pipeline.sink",
            "--mode", "effectively-once",
            "--lake-dir", lake_dir,
            "--state-db", state_db,
            "-B", "1000",
            "-T", "0.5",
            "--topic", redpanda_topic,
            "--require-kafka",
            "--bootstrap-servers", BOOTSTRAP_SERVERS,
        ]
        return subprocess.Popen(cmd, cwd=BASE_DIR, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    proc = launch_sink()
    time.sleep(0.5)
    hard_kill_process(proc)

    proc_restart = launch_sink()
    try:
        proc_restart.wait(timeout=60)
    except subprocess.TimeoutExpired:
        hard_kill_process(proc_restart)

    stats = lake_stats(lake_dir)
    lake_ids = lake_row_ids(lake_dir)
    duplicates = stats["rows"] - stats["unique_row_ids"]
    missing = len(produced_ids - lake_ids)

    assert duplicates == 0
    assert missing == 0
    assert stats["unique_row_ids"] == 10000
