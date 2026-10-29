"""Small-files problem and compaction benchmark.

Writes many small micro-batch files into the lake to reproduce the small-files problem,
measures DuckDB query performance, runs the compaction job, and measures query performance again.
"""

import argparse
import glob
import json
import os
import shutil
import sys
import time
from typing import Any, Dict

import duckdb

from pipeline.compaction import compact_lake
from pipeline.producer import run_producer
from pipeline.queries import query_avg_fare_per_distance, query_hourly_trips, query_top_10_pairs
from pipeline.sink import SinkConsumer

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "yellow_tripdata_2024-01.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")


def run_compaction_benchmark(records: int = 15000, micro_batch_size: int = 50) -> Dict[str, Any]:
    print(f"\n--- Compaction Benchmark (records={records:,}, batch={micro_batch_size}) ---")

    test_lake = os.path.join(BASE_DIR, "lake_compaction_bench")
    test_broker = os.path.join(BASE_DIR, "lake_compaction_broker")
    state_db = os.path.join(test_lake, "sink_state.duckdb")

    if os.path.exists(test_lake):
        shutil.rmtree(test_lake, ignore_errors=True)
    if os.path.exists(test_broker):
        shutil.rmtree(test_broker, ignore_errors=True)

    os.environ["LOCAL_BROKER_DIR"] = test_broker
    topic = "trips_compact"

    print("Staging records into local broker...")
    prod_summary = run_producer(
        source_path=DATA_PATH,
        topic=topic,
        max_records=records,
        force_local=True,
        save_stats=None,
    )

    print("Flushing micro-batches to create small files...")
    sink = SinkConsumer(
        mode="effectively-once",
        batch_size=micro_batch_size,
        batch_timeout=0.01,
        lake_dir=test_lake,
        state_db_path=state_db,
        topic=topic,
        force_local=True,
    )
    sink.run(max_messages=records, max_seconds=30.0)

    # Count files and size before compaction
    before_files = glob.glob(os.path.join(test_lake, "**", "*.parquet"), recursive=True)
    before_size_bytes = sum(os.path.getsize(f) for f in before_files)
    before_count = len(before_files)
    print(f"Before Compaction: {before_count} files ({before_size_bytes / 1024 / 1024:.2f} MB)")

    con = duckdb.connect()

    # Query times before compaction
    _, q1_before = query_hourly_trips(test_lake, con)
    _, q2_before = query_avg_fare_per_distance(test_lake, con)
    _, q3_before = query_top_10_pairs(test_lake, con)
    total_before = q1_before + q2_before + q3_before

    # Run compaction
    print("Running compaction...")
    t_comp_start = time.perf_counter()
    comp_result = compact_lake(lake_dir=test_lake, state_db_path=state_db)
    compaction_duration = time.perf_counter() - t_comp_start

    # Count files and size after compaction
    after_files = glob.glob(os.path.join(test_lake, "**", "*.parquet"), recursive=True)
    after_size_bytes = sum(os.path.getsize(f) for f in after_files)
    after_count = len(after_files)
    print(f"After Compaction: {after_count} files ({after_size_bytes / 1024 / 1024:.2f} MB)")

    # Query times after compaction
    _, q1_after = query_hourly_trips(test_lake, con)
    _, q2_after = query_avg_fare_per_distance(test_lake, con)
    _, q3_after = query_top_10_pairs(test_lake, con)
    total_after = q1_after + q2_after + q3_after

    con.close()

    speedup = total_before / max(0.0001, total_after)

    summary = {
        "files_before": before_count,
        "files_after": after_count,
        "size_before_mb": round(before_size_bytes / 1024 / 1024, 2),
        "size_after_mb": round(after_size_bytes / 1024 / 1024, 2),
        "compaction_time_sec": round(compaction_duration, 3),
        "query_results": {
            "q1_hourly_trips": {
                "before_ms": round(q1_before * 1000, 2),
                "after_ms": round(q1_after * 1000, 2),
                "speedup": round(q1_before / max(0.0001, q1_after), 2),
            },
            "q2_avg_fare": {
                "before_ms": round(q2_before * 1000, 2),
                "after_ms": round(q2_after * 1000, 2),
                "speedup": round(q2_before / max(0.0001, q2_after), 2),
            },
            "q3_top_pairs": {
                "before_ms": round(q3_before * 1000, 2),
                "after_ms": round(q3_after * 1000, 2),
                "speedup": round(q3_before / max(0.0001, q3_after), 2),
            },
            "total_queries": {
                "before_ms": round(total_before * 1000, 2),
                "after_ms": round(total_after * 1000, 2),
                "overall_speedup": round(speedup, 2),
            },
        },
    }

    print("\n--- Compaction Benchmark Results ---")
    print(f"Files: {before_count} -> {after_count} ({summary['size_before_mb']} MB -> {summary['size_after_mb']} MB)")
    print(f"Total Query Time: {summary['query_results']['total_queries']['before_ms']} ms -> {summary['query_results']['total_queries']['after_ms']} ms ({speedup:.2f}x speedup)")

    out_path = os.path.join(RESULTS_DIR, "compaction_benchmark.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Compaction benchmark runner.")
    parser.add_argument("--records", type=int, default=15000, help="Records to produce")
    parser.add_argument("--micro-batch", type=int, default=50, help="Micro-batch size in rows")
    args = parser.parse_args()

    run_compaction_benchmark(records=args.records, micro_batch_size=args.micro_batch)


if __name__ == "__main__":
    main()
