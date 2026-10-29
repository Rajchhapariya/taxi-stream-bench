"""Latency vs batch size benchmark.

Varies micro-batch size B (1k, 10k, 50k rows).
Measures p50, p95, and p99 end-to-end latency from produce_ts to Parquet commit,
plus sustained ingestion throughput across 3 runs.
"""

import argparse
import json
import os
import shutil
import statistics
import sys
import threading
import time
from typing import Any, Dict, List

import numpy as np

from pipeline.broker import create_consumer, create_producer
from pipeline.producer import run_producer
from pipeline.sink import SinkConsumer

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "yellow_tripdata_2024-01.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")


def run_single_latency_trial(batch_size: int, num_records: int = 50000) -> Dict[str, Any]:
    trial_lake = os.path.join(BASE_DIR, "lake_latency_trial")
    trial_broker = os.path.join(BASE_DIR, "lake_latency_broker")
    state_db = os.path.join(trial_lake, "sink_state.duckdb")

    if os.path.exists(trial_lake):
        shutil.rmtree(trial_lake, ignore_errors=True)
    if os.path.exists(trial_broker):
        shutil.rmtree(trial_broker, ignore_errors=True)

    os.environ["LOCAL_BROKER_DIR"] = trial_broker
    topic = "trips_lat"

    sink = SinkConsumer(
        mode="effectively-once",
        batch_size=batch_size,
        batch_timeout=30.0,
        lake_dir=trial_lake,
        state_db_path=state_db,
        topic=topic,
        force_local=True,
    )

    sink_done = threading.Event()
    sink_summary = {}

    def sink_worker():
        nonlocal sink_summary
        sink_summary = sink.run(max_messages=num_records, max_seconds=40.0)
        sink_done.set()

    t_sink = threading.Thread(target=sink_worker, daemon=True)
    t_sink.start()

    time.sleep(0.1)

    t0 = time.perf_counter()
    prod_summary = run_producer(
        source_path=DATA_PATH,
        topic=topic,
        max_records=num_records,
        force_local=True,
        save_stats=None,
    )

    t_sink.join(timeout=45.0)
    total_duration = max(0.001, time.perf_counter() - t0)

    latencies = sink.latencies_ms
    if not latencies:
        p50 = p95 = p99 = 0.0
    else:
        lat_arr = np.array(latencies)
        p50 = float(np.percentile(lat_arr, 50))
        p95 = float(np.percentile(lat_arr, 95))
        p99 = float(np.percentile(lat_arr, 99))

    rows_written = sink_summary.get("total_rows_committed", 0)
    throughput = rows_written / total_duration

    return {
        "batch_size": batch_size,
        "rows_processed": rows_written,
        "duration_sec": round(total_duration, 3),
        "throughput_events_per_sec": round(throughput, 1),
        "p50_ms": round(p50, 1),
        "p95_ms": round(p95, 1),
        "p99_ms": round(p99, 1),
    }


def run_latency_benchmark(
    batch_sizes: List[int] = [1000, 10000, 50000],
    repetitions: int = 3,
    records_per_run: int = 50000,
) -> Dict[str, Any]:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    results = {}

    for b in batch_sizes:
        print(f"\nEvaluating batch size B = {b:,} rows ({repetitions} runs)...")
        trials = []
        for rep in range(1, repetitions + 1):
            print(f"  Run {rep}/{repetitions}...")
            trial = run_single_latency_trial(batch_size=b, num_records=records_per_run)
            trials.append(trial)
            time.sleep(0.2)

        p50_list = [t["p50_ms"] for t in trials]
        p95_list = [t["p95_ms"] for t in trials]
        p99_list = [t["p99_ms"] for t in trials]
        tp_list = [t["throughput_events_per_sec"] for t in trials]

        results[str(b)] = {
            "batch_size": b,
            "median_p50_ms": round(statistics.median(p50_list), 1),
            "median_p95_ms": round(statistics.median(p95_list), 1),
            "min_p95_ms": round(min(p95_list), 1),
            "max_p95_ms": round(max(p95_list), 1),
            "median_p99_ms": round(statistics.median(p99_list), 1),
            "median_throughput": round(statistics.median(tp_list), 1),
            "trials": trials,
        }
        print(f"  Result B={b}: p50={results[str(b)]['median_p50_ms']}ms, p95={results[str(b)]['median_p95_ms']}ms, throughput={results[str(b)]['median_throughput']} ev/s")

    out_path = os.path.join(RESULTS_DIR, "latency_benchmark.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    return results


def main():
    parser = argparse.ArgumentParser(description="Latency benchmark runner.")
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=[1000, 10000, 50000], help="Batch sizes in rows")
    parser.add_argument("--repetitions", type=int, default=3, help="Runs per batch size")
    parser.add_argument("--records", type=int, default=50000, help="Records per run")
    args = parser.parse_args()

    run_latency_benchmark(batch_sizes=args.batch_sizes, repetitions=args.repetitions, records_per_run=args.records)


if __name__ == "__main__":
    main()
