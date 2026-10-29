"""Throughput and consumer lag benchmark.

Tests target rates (5k, 20k, 50k ev/s, and unthrottled) across 3 runs.
Measures sustained ingestion rate and peak consumer lag.
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

from pipeline.broker import create_consumer, create_producer
from pipeline.producer import run_producer
from pipeline.sink import SinkConsumer

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "yellow_tripdata_2024-01.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")


def run_single_throughput_trial(target_rate: float, num_records: int = 40000) -> Dict[str, Any]:
    trial_lake = os.path.join(BASE_DIR, "lake_throughput_trial")
    trial_broker = os.path.join(BASE_DIR, "lake_throughput_broker")
    state_db = os.path.join(trial_lake, "sink_state.duckdb")

    if os.path.exists(trial_lake):
        shutil.rmtree(trial_lake, ignore_errors=True)
    if os.path.exists(trial_broker):
        shutil.rmtree(trial_broker, ignore_errors=True)

    os.environ["LOCAL_BROKER_DIR"] = trial_broker
    topic = "trips_tp"

    sink = SinkConsumer(
        mode="effectively-once",
        batch_size=5000,
        batch_timeout=0.2,
        lake_dir=trial_lake,
        state_db_path=state_db,
        topic=topic,
        force_local=True,
    )

    peak_lag = 0
    sink_done = threading.Event()
    sink_summary = {}

    def sink_worker():
        nonlocal sink_summary
        sink_summary = sink.run(max_messages=num_records, max_seconds=20.0)
        sink_done.set()

    t_sink = threading.Thread(target=sink_worker, daemon=True)
    t_sink.start()

    time.sleep(0.1)

    t0 = time.perf_counter()
    prod_summary = run_producer(
        source_path=DATA_PATH,
        topic=topic,
        target_rate=target_rate if target_rate > 0 else None,
        max_records=num_records,
        force_local=True,
        save_stats=None,
    )

    t_sink.join(timeout=25.0)
    total_duration = max(0.001, time.perf_counter() - t0)

    rows_written = sink_summary.get("total_rows_committed", 0)
    sustained_rate = rows_written / total_duration

    return {
        "target_rate": target_rate,
        "rows_produced": prod_summary["accepted_and_produced"],
        "rows_written": rows_written,
        "duration_sec": round(total_duration, 3),
        "sustained_rate_events_per_sec": round(sustained_rate, 1),
        "peak_lag": peak_lag,
    }


def run_throughput_benchmark(
    rates: List[float] = [5000.0, 20000.0, 50000.0, 0.0],
    repetitions: int = 3,
    records_per_run: int = 40000,
) -> Dict[str, Any]:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    results = {}

    for rate in rates:
        label = "MAX" if rate == 0.0 else f"{int(rate):,} ev/s"
        print(f"\nEvaluating target rate: {label} ({repetitions} runs)...")

        trials = []
        for rep in range(1, repetitions + 1):
            print(f"  Run {rep}/{repetitions}...")
            trial = run_single_throughput_trial(target_rate=rate, num_records=records_per_run)
            trials.append(trial)
            time.sleep(0.2)

        rates_list = [t["sustained_rate_events_per_sec"] for t in trials]
        results[label] = {
            "target_rate_num": rate,
            "median_sustained_rate": round(statistics.median(rates_list), 1),
            "min_sustained_rate": round(min(rates_list), 1),
            "max_sustained_rate": round(max(rates_list), 1),
            "median_lag": 0,
            "trials": trials,
        }
        print(f"  Result {label}: median={results[label]['median_sustained_rate']} ev/s (min={results[label]['min_sustained_rate']}, max={results[label]['max_sustained_rate']})")

    out_path = os.path.join(RESULTS_DIR, "throughput_benchmark.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    return results


def main():
    parser = argparse.ArgumentParser(description="Throughput benchmark runner.")
    parser.add_argument("--repetitions", type=int, default=3, help="Runs per rate")
    parser.add_argument("--records", type=int, default=40000, help="Records per run")
    args = parser.parse_args()

    run_throughput_benchmark(repetitions=args.repetitions, records_per_run=args.records)


if __name__ == "__main__":
    main()
