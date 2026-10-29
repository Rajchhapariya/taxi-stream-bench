"""Crash injection test harness.

Kills the sink process with SIGKILL / TerminateProcess 10 times per run
across 3 random seeds for both delivery modes, then checks DuckDB for duplicate
and missing row_id entries against the producer's accepted count.
"""

import argparse
import glob
import json
import os
import random
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List

import duckdb

from pipeline.broker import LocalBrokerEngine
from pipeline.producer import run_producer

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "yellow_tripdata_2024-01.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")


def run_single_crash_experiment(
    mode: str,
    seed: int,
    num_records: int = 50000,
    batch_size: int = 1000,
    num_kills: int = 10,
) -> Dict[str, Any]:
    print(f"\n--- Crash Run: mode={mode}, seed={seed}, kills={num_kills} ---")

    random.seed(seed)

    lake_dir = os.path.join(BASE_DIR, "lake_crash", f"{mode}_s{seed}")
    broker_state_dir = os.path.join(BASE_DIR, "lake_crash", f"{mode}_s{seed}_broker")
    state_db = os.path.join(lake_dir, "sink_state.duckdb")

    if os.path.exists(lake_dir):
        shutil.rmtree(lake_dir, ignore_errors=True)
    if os.path.exists(broker_state_dir):
        shutil.rmtree(broker_state_dir, ignore_errors=True)

    os.makedirs(lake_dir, exist_ok=True)
    os.makedirs(broker_state_dir, exist_ok=True)

    os.environ["LOCAL_BROKER_DIR"] = broker_state_dir
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR
    env["LOCAL_BROKER_DIR"] = broker_state_dir

    print(f"Staging {num_records:,} records in local broker...")
    prod_summary = run_producer(
        source_path=DATA_PATH,
        topic="trips_crash",
        max_records=num_records,
        force_local=True,
        save_stats=None,
    )
    total_produced = prod_summary["accepted_and_produced"]

    def launch_sink(crash_prob: float = 0.08):
        cmd = [
            sys.executable,
            "-u",
            "-m", "pipeline.sink",
            "--mode", mode,
            "--lake-dir", lake_dir,
            "--state-db", state_db,
            "-B", str(batch_size),
            "-T", "0.5",
            "--topic", "trips_crash",
            "--crash-prob", str(crash_prob),
            "--force-local",
        ]
        return subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    current_proc = launch_sink()

    for kill_i in range(1, num_kills + 1):
        # Stagger kill intervals between 80ms and 350ms to catch active writes
        sleep_duration = random.uniform(0.08, 0.35)
        time.sleep(sleep_duration)

        if current_proc.poll() is None:
            print(f"Kill {kill_i}/{num_kills}: terminating sink (PID {current_proc.pid})")
            current_proc.kill()
            try:
                current_proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                pass
            time.sleep(0.15)
        else:
            print(f"Kill {kill_i}/{num_kills}: sink exited early (code {current_proc.returncode})")

        current_proc = launch_sink(crash_prob=0.08)

    # Let the final sink process drain remaining records
    print("All kills fired. Waiting for sink to drain queue...")
    try:
        stdout, stderr = current_proc.communicate(timeout=25.0)
    except subprocess.TimeoutExpired:
        current_proc.kill()
        stdout, stderr = current_proc.communicate()

    # Query Parquet lake to measure duplicate and missing row_ids
    norm_lake = lake_dir.replace("\\", "/")
    pattern = f"{norm_lake}/*/*.parquet"

    parquet_files = glob.glob(os.path.join(lake_dir, "**", "*.parquet"), recursive=True)
    con = duckdb.connect()

    if parquet_files:
        stats = con.execute(f"""
            SELECT 
                count(*) as total_rows,
                count(distinct row_id) as unique_rows,
                count(*) - count(distinct row_id) as duplicate_rows,
                min(row_id) as min_row,
                max(row_id) as max_row
            FROM read_parquet('{pattern}')
        """).fetchone()

        total_in_lake = stats[0]
        unique_rows = stats[1]
        duplicate_rows = stats[2]
        missing_rows = max(0, total_produced - unique_rows)
    else:
        total_in_lake = 0
        unique_rows = 0
        duplicate_rows = 0
        missing_rows = total_produced

    con.close()

    result = {
        "mode": mode,
        "seed": seed,
        "kills_attempted": num_kills,
        "total_produced": total_produced,
        "total_in_lake": total_in_lake,
        "unique_rows": unique_rows,
        "duplicate_rows": duplicate_rows,
        "missing_rows": missing_rows,
        "file_count": len(parquet_files),
    }

    print(f"Result [{mode} seed={seed}]: Lake={total_in_lake:,} | Dups={duplicate_rows:,} | Missing={missing_rows:,} | Files={len(parquet_files)}")
    return result


def run_full_crash_suite(
    seeds: List[int] = [42, 100, 2024],
    num_kills: int = 10,
    records_per_run: int = 50000,
) -> Dict[str, Any]:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    all_results = []

    for mode in ["at-least-once", "effectively-once"]:
        for seed in seeds:
            res = run_single_crash_experiment(
                mode=mode,
                seed=seed,
                num_records=records_per_run,
                num_kills=num_kills,
            )
            all_results.append(res)

    def compute_summary(mode_str: str) -> Dict[str, Any]:
        runs = [r for r in all_results if r["mode"] == mode_str]
        n = len(runs)
        return {
            "runs_evaluated": n,
            "mean_duplicate_rows": round(sum(r["duplicate_rows"] for r in runs) / n, 2),
            "mean_missing_rows": round(sum(r["missing_rows"] for r in runs) / n, 2),
            "mean_files": round(sum(r["file_count"] for r in runs) / n, 1),
        }

    suite_summary = {
        "seeds": seeds,
        "kills_per_run": num_kills,
        "mode_a_at_least_once": compute_summary("at-least-once"),
        "mode_b_effectively_once": compute_summary("effectively-once"),
        "all_runs": all_results,
    }

    out_path = os.path.join(RESULTS_DIR, "crash_benchmark.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(suite_summary, f, indent=2)

    print(f"\nCrash test suite finished. Saved to {out_path}")
    return suite_summary


def main():
    parser = argparse.ArgumentParser(description="Crash injection benchmark harness.")
    parser.add_argument("--seeds", type=int, nargs="+", default=[42, 100, 2024], help="Random seeds")
    parser.add_argument("--kills", type=int, default=10, help="Forced kills per run")
    parser.add_argument("--records", type=int, default=50000, help="Test rows produced per run")
    args = parser.parse_args()

    run_full_crash_suite(seeds=args.seeds, num_kills=args.kills, records_per_run=args.records)


if __name__ == "__main__":
    main()
