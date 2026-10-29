"""End-to-end latency versus batch size.

Latency of a row = time the sink committed its batch file minus the produce timestamp the producer
stamped on it. The producer runs in its own process at a fixed rate that is below what the sink
can absorb, so queueing backlog does not inflate the numbers (the first version replayed the whole
input at once and measured the backlog). The sink flushes a partition when it holds B rows or when
T seconds have passed since that partition's last flush, whichever comes first.

Excluded from the percentiles: every batch committed during the first 30 s of production (warm-up)
and the partial batches flushed when the run ends. The flush-reason counts show how many batches were closed by size and
how many by the timer.
"""

import argparse
import os
import shutil
import statistics
import threading
import time
from typing import Any, Dict, List, Optional

from bench.common import (BASE_DIR, TrialBudget, clear_partial, load_partial, percentile, save_partial,
                          start_producer_process, topic_end_total, write_result)
from pipeline.sink import SinkConsumer

WARMUP_S = 30.0
TOPIC = "trips_lat"
PARTITIONS = 6


def run_single_latency_trial(batch_size: int, rate: float, duration_s: float, batch_timeout: float,
                             warmup_s: float = 30.0) -> Dict[str, Any]:
    lake = os.path.join(BASE_DIR, "lake_latency_trial")
    broker_dir = os.path.join(BASE_DIR, "lake_latency_broker")
    stats_out = os.path.join(BASE_DIR, "lake_latency_producer_stats.json")
    for d in (lake, broker_dir):
        shutil.rmtree(d, ignore_errors=True)
    os.environ["LOCAL_BROKER_DIR"] = broker_dir
    records = int(rate * (duration_s + warmup_s))

    sink = SinkConsumer(mode="effectively-once", batch_size=batch_size, batch_timeout=batch_timeout,
                        lake_dir=lake, state_db_path=os.path.join(lake, "sink_state.duckdb"),
                        topic=TOPIC, force_local=True)
    t_sink = threading.Thread(target=lambda: sink.run(idle_exit_polls=None), daemon=True)
    t_sink.start()
    time.sleep(0.2)

    prod = start_producer_process(TOPIC, broker_dir, rate, records, stats_out)
    t_prod_start = time.time()
    max_lag = 0
    while prod.poll() is None:
        time.sleep(0.5)
        max_lag = max(max_lag, topic_end_total(broker_dir, TOPIC) - sink.messages_polled)

    produced_total = topic_end_total(broker_dir, TOPIC)
    deadline = time.time() + 60
    while sink.messages_polled < produced_total and time.time() < deadline:
        time.sleep(0.1)
    drained = sink.messages_polled >= produced_total
    sink.running = False
    t_sink.join(timeout=30)

    # Batches committed during the first warmup_s seconds of production are dropped.
    lats: List[float] = []
    reasons = {"size": 0, "timeout": 0, "final": 0}
    for partition, reason, batch_lat, committed_at in sink.batch_latencies:
        reasons[reason] += 1
        if committed_at < t_prod_start + warmup_s or reason == "final":
            continue
        lats.extend(batch_lat)

    return {
        "batch_size": batch_size,
        "warmup_s": warmup_s,
        "rows_produced": produced_total,
        "rows_in_percentiles": len(lats),
        "p50_ms": round(percentile(lats, 50), 1),
        "p95_ms": round(percentile(lats, 95), 1),
        "p99_ms": round(percentile(lats, 99), 1),
        "batches_closed_by_size": reasons["size"],
        "batches_closed_by_timer": reasons["timeout"],
        "partial_batches_at_end": reasons["final"],
        "max_lag_rows": max_lag,
        "sink_drained": drained,
    }


def run_latency_benchmark(batch_sizes: List[int], repetitions: int, rate: float, duration_s: float,
                          batch_timeout: float, max_new: Optional[int] = None) -> Optional[Dict[str, Any]]:
    saved: Dict[str, Any] = load_partial("latency")
    budget = TrialBudget(max_new)
    results: Dict[str, Any] = {}
    for b in batch_sizes:
        print(f"\nB = {b:,} rows, T = {batch_timeout}s, rate = {rate:,.0f} ev/s ({repetitions} runs)", flush=True)
        trials = []
        for rep in range(1, repetitions + 1):
            key = f"{b}:{rep}"
            if key not in saved:
                if not budget.take():
                    print("Stopped after budget; run again to continue.")
                    return None
                saved[key] = run_single_latency_trial(b, rate, duration_s, batch_timeout, WARMUP_S)
                save_partial("latency", saved)
            t = saved[key]
            print(f"  run {rep}: p50={t['p50_ms']} p95={t['p95_ms']} p99={t['p99_ms']} ms | "
                  f"size/timer/final batches={t['batches_closed_by_size']}/{t['batches_closed_by_timer']}/"
                  f"{t['partial_batches_at_end']} | max lag={t['max_lag_rows']}", flush=True)
            trials.append(t)
        med = lambda k: round(statistics.median(x[k] for x in trials), 1)
        results[str(b)] = {
            "batch_size": b,
            "median_p50_ms": med("p50_ms"),
            "median_p95_ms": med("p95_ms"),
            "min_p95_ms": min(x["p95_ms"] for x in trials),
            "max_p95_ms": max(x["p95_ms"] for x in trials),
            "median_p99_ms": med("p99_ms"),
            "trials": trials,
        }
    write_result("latency_benchmark.json", {
        "parameters": {
            "producer_rate_events_per_s": rate, "duration_s": duration_s, "batch_timeout_s": batch_timeout,
            "repetitions": repetitions, "partitions": PARTITIONS, "warmup_s": WARMUP_S,
            "batch_size_is_per_partition": True,
            "spec_batch_sizes": [1000, 10000, 50000],
            "note": "Sizes outside the spec set are extra points to show where the timer takes over.",
        },
        "by_batch_size": results,
    }, force_local=True)
    clear_partial("latency")
    return results


def main():
    parser = argparse.ArgumentParser(description="Latency benchmark.")
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=[250, 1000, 3000, 10000, 50000])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--rate", type=float, default=2000.0)
    parser.add_argument("--duration", type=float, default=40.0)
    parser.add_argument("--batch-timeout", type=float, default=10.0)
    parser.add_argument("--max-new", type=int, default=None)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()
    if args.fresh:
        clear_partial("latency")
    run_latency_benchmark(args.batch_sizes, args.repetitions, args.rate, args.duration, args.batch_timeout, args.max_new)


if __name__ == "__main__":
    main()
