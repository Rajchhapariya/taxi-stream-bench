"""Crash injection benchmark: at-least-once versus effectively-once.

Per run: stage N accepted trips on the topic, start the sink, hard-kill it (SIGKILL, or
TerminateProcess on Windows) at random moments 10 times, restarting it after each kill, then
run one uninjected sink until the topic is drained. Afterwards the lake is compared with the
row_ids the producer put on the topic:
  duplicate rows = rows in the lake minus distinct row_ids in the lake
  missing rows   = produced row_ids that are not in the lake

Configurations
  at-least-once                  Mode A: write the file, then commit the offset to the broker.
  effectively-once               Mode B: offset-range file names, atomic rename, DuckDB manifest,
                                 resume from DuckDB offsets, and delete files the manifest does
                                 not list on restart.
  effectively-once-no-reconcile  Mode B exactly as the build spec words it: temp files are deleted
                                 on restart but a renamed file missing from the manifest is kept.

Variants
  sigkill-only            only the random kills. The window between the file write and the offset
                          record is tens of milliseconds, so most kills miss it.
  sigkill+boundary-crash  the same kills, plus each batch has an 8 percent chance of the process
                          dying inside that window. The injected exit skips all cleanup.
  ...+timer-batches       as above with a 0.02 s batch timer, so most batches are closed by the
                          timer and their offset ranges depend on timing. A replay then produces
                          different file names from the batch that was interrupted.
"""

import argparse
import glob
import os
import random
import shutil
import subprocess
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from bench.common import (BASE_DIR, DATA_PATH, TrialBudget, clear_partial, hard_kill_process,
                          lake_row_ids, lake_stats, load_partial, produced_row_ids,
                          result_filename, save_partial, write_result)
from pipeline.broker import BrokerUnavailableError, create_kafka_topic, delete_kafka_topic, kafka_usable
from pipeline.producer import run_producer

CONFIGS: List[Tuple[str, str, bool]] = [
    ("at-least-once", "at-least-once", True),
    ("effectively-once", "effectively-once", True),
    ("effectively-once-no-reconcile", "effectively-once", False),
]
# (name, boundary crash probability, batch timer in seconds, records per run)
VARIANTS: List[Tuple[str, float, float, int]] = [
    ("sigkill-only", 0.0, 0.5, 30000),
    ("sigkill+boundary-crash", 0.08, 0.5, 30000),
    ("sigkill+boundary-crash+timer-batches", 0.08, 0.05, 8000),
]
TOPIC = "trips_crash"


def run_single_crash_experiment(config: Tuple[str, str, bool], variant: Tuple[str, float, float, int], seed: int,
                                batch_size: int, num_kills: int, broker: str = "local",
                                bootstrap_servers: str = "localhost:9092") -> Dict[str, Any]:
    cfg_name, mode, reconcile = config
    var_name, crash_prob, batch_timeout, num_records = variant
    tag = f"{cfg_name}_{var_name}_s{seed}".replace("+", "-")
    print(f"\n--- {cfg_name} | {var_name} | seed {seed} | {num_kills} kills [{broker}] ---", flush=True)

    if broker == "kafka":
        if not kafka_usable(bootstrap_servers):
            raise BrokerUnavailableError(f"Broker unavailable at {bootstrap_servers} for kafka-mode crash test")
        clean_tag = tag.replace("-", "_").replace(".", "_")
        trial_topic = f"trips_crash_{clean_tag}_{uuid.uuid4().hex[:6]}"
        create_kafka_topic(trial_topic, bootstrap_servers, num_partitions=6)
    else:
        trial_topic = TOPIC

    try:
        rng = random.Random(seed)
        lake_dir = os.path.join(BASE_DIR, "lake_crash", f"{tag}_{broker}")
        broker_dir = os.path.join(BASE_DIR, "lake_crash", f"{tag}_{broker}_broker")
        state_db = os.path.join(lake_dir, "sink_state.duckdb")
        for d in (lake_dir, broker_dir):
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d, exist_ok=True)

        os.environ["LOCAL_BROKER_DIR"] = broker_dir
        env = os.environ.copy()
        env["PYTHONPATH"] = BASE_DIR
        env["LOCAL_BROKER_DIR"] = broker_dir

        prod = run_producer(source_path=DATA_PATH, topic=trial_topic, max_records=num_records,
                            force_local=(broker == "local"), require_kafka=(broker == "kafka"),
                            bootstrap_servers=bootstrap_servers, return_row_ids=True, save_stats=None)
        produced_ids = prod.get("produced_row_ids")
        if produced_ids is None:
            produced_ids = produced_row_ids(broker_dir, trial_topic)

        launches = {"n": 0}

        def launch(prob: float) -> subprocess.Popen:
            launches["n"] += 1
            cmd = [
                sys.executable, "-u", "-m", "pipeline.sink", "--mode", mode,
                "--lake-dir", lake_dir, "--state-db", state_db,
                "-B", str(batch_size), "-T", str(batch_timeout), "--topic", trial_topic,
                "--group-id", f"{trial_topic}_grp",
                "--crash-prob", str(prob), "--crash-seed", str(seed * 1000 + launches["n"]),
            ]
            if broker == "kafka":
                cmd.extend(["--require-kafka", "--bootstrap-servers", bootstrap_servers])
            else:
                cmd.append("--force-local")
            if not reconcile:
                cmd.append("--no-reconcile")
            return subprocess.Popen(cmd, cwd=BASE_DIR, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        proc = launch(crash_prob)
        kills_delivered = 0
        exits_before_kill = 0
        injected_exits = 0

        for _ in range(num_kills):
            deadline = time.time() + rng.uniform(0.3, 1.2)
            while time.time() < deadline:
                if proc.poll() is not None:
                    # the sink ended by itself (injected crash, or it drained the topic): restart it
                    exits_before_kill += 1
                    if proc.returncode in (42, 43, 44):
                        injected_exits += 1
                    proc = launch(crash_prob)
                time.sleep(0.01)
            if proc.poll() is None:
                hard_kill_process(proc)
                kills_delivered += 1
            else:
                exits_before_kill += 1
                if proc.returncode in (42, 43, 44):
                    injected_exits += 1
            proc = launch(crash_prob)

        # Final run: no injection, runs until the topic is drained and the sink exits on its own.
        hard_kill_process(proc)
        final = launch(0.0)
        try:
            final.wait(timeout=240)
            drain_completed = final.returncode == 0
        except subprocess.TimeoutExpired:
            hard_kill_process(final)
            drain_completed = False

        stats = lake_stats(lake_dir)
        lake_ids = lake_row_ids(lake_dir)
        result = {
            "config": cfg_name,
            "variant": var_name,
            "seed": seed,
            "kills_requested": num_kills,
            "kills_delivered": kills_delivered,
            "exits_before_kill": exits_before_kill,
            "injected_exits": injected_exits,
            "sink_launches": launches["n"],
            "drain_completed": drain_completed,
            "total_produced": prod["accepted_and_produced"],
            "distinct_produced_ids": len(produced_ids),
            "total_in_lake": stats["rows"],
            "unique_in_lake": stats["unique_row_ids"],
            "duplicate_rows": stats["rows"] - stats["unique_row_ids"],
            "missing_rows": len(produced_ids - lake_ids),
            "unexpected_rows": len(lake_ids - produced_ids),
            "file_count": stats["files"],
        }
        print(f"  lake={result['total_in_lake']:,} dup={result['duplicate_rows']:,} "
              f"missing={result['missing_rows']:,} kills={kills_delivered} injected={injected_exits} "
              f"drained={drain_completed}", flush=True)
        return result
    finally:
        if broker == "kafka":
            delete_kafka_topic(trial_topic, bootstrap_servers)


def summarise(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for cfg, _, _ in CONFIGS:
        for var, _, _, _ in VARIANTS:
            rs = [r for r in runs if r["config"] == cfg and r["variant"] == var]
            if not rs:
                continue
            n = len(rs)
            out[f"{cfg} | {var}"] = {
                "runs": n,
                "mean_duplicate_rows": round(sum(r["duplicate_rows"] for r in rs) / n, 2),
                "max_duplicate_rows": max(r["duplicate_rows"] for r in rs),
                "mean_missing_rows": round(sum(r["missing_rows"] for r in rs) / n, 2),
                "total_kills_delivered": sum(r["kills_delivered"] for r in rs),
                "total_injected_exits": sum(r["injected_exits"] for r in rs),
                "all_drains_completed": all(r["drain_completed"] for r in rs),
            }
    return out


def run_full_crash_suite(seeds: List[int], num_kills: int, batch_size: int,
                         max_new: Optional[int] = None, broker: str = "local",
                         bootstrap_servers: str = "localhost:9092") -> Optional[Dict[str, Any]]:
    partial_key = f"crash_{broker}" if broker != "local" else "crash"
    partial = load_partial(partial_key)
    runs: List[Dict[str, Any]] = partial.get("runs", [])
    done = {(r["config"], r["variant"], r["seed"]) for r in runs}
    budget = TrialBudget(max_new)
    for variant in VARIANTS:
        for config in CONFIGS:
            for seed in seeds:
                if (config[0], variant[0], seed) in done:
                    continue
                if not budget.take():
                    print(f"\nStopped after budget; {len(runs)} runs saved. Run again to continue.")
                    return None
                runs.append(run_single_crash_experiment(config, variant, seed, batch_size, num_kills,
                                                        broker=broker, bootstrap_servers=bootstrap_servers))
                save_partial(partial_key, {"runs": runs})
    out_name = result_filename("crash_benchmark.json", broker=broker)
    doc = write_result(out_name, {
        "parameters": {
            "seeds": seeds, "kills_per_run": num_kills,
            "batch_size": batch_size, "commit_delay_s": 0.02,
            "variants": {v[0]: {"boundary_crash_probability": v[1], "batch_timeout_s": v[2], "records_per_run": v[3]} for v in VARIANTS},
        },
        "summary": summarise(runs),
        "runs": runs,
    }, force_local=(broker == "local"), require_kafka=(broker == "kafka"),
       bootstrap_servers=bootstrap_servers, broker=broker)
    clear_partial(partial_key)
    print(f"\nSaved results/{out_name}")
    return doc


def main():
    parser = argparse.ArgumentParser(description="Crash injection benchmark.")
    parser.add_argument("--broker", type=str, choices=["local", "kafka"], default="local",
                        help="Broker engine ('local' for disk emulator, 'kafka' for Redpanda/Kafka)")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--seeds", type=int, nargs="+", default=[42, 100, 2024])
    parser.add_argument("--kills", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--max-new", type=int, default=None, help="Stop after N new runs; rerun to resume")
    parser.add_argument("--fresh", action="store_true", help="Discard saved partial runs")
    args = parser.parse_args()
    partial_key = f"crash_{args.broker}" if args.broker != "local" else "crash"
    if args.fresh:
        clear_partial(partial_key)
    run_full_crash_suite(args.seeds, args.kills, args.batch_size, args.max_new,
                         broker=args.broker, bootstrap_servers=args.bootstrap_servers)


if __name__ == "__main__":
    main()
