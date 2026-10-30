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
import sys
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from bench.common import (BASE_DIR, TrialBudget, clear_partial, load_partial, percentile,
                          result_filename, save_partial, start_producer_process,
                          topic_end_total, topic_end_total_agnostic, write_result)
from pipeline.broker import BrokerUnavailableError, create_kafka_topic, delete_kafka_topic, kafka_usable
from pipeline.sink import SinkConsumer

WARMUP_S = 30.0
TOPIC = "trips_lat"
PARTITIONS = 6


def run_single_latency_trial(batch_size: int, rate: float, duration_s: float, batch_timeout: float,
                             warmup_s: float = 30.0, broker: str = "local",
                             bootstrap_servers: str = "localhost:9092") -> Dict[str, Any]:
    if broker == "kafka":
        if not kafka_usable(bootstrap_servers):
            raise BrokerUnavailableError(f"Broker unavailable at {bootstrap_servers} for kafka-mode latency benchmark")
        trial_topic = f"trips_lat_b{batch_size}_{uuid.uuid4().hex[:6]}"
        create_kafka_topic(trial_topic, bootstrap_servers, num_partitions=PARTITIONS)
    else:
        trial_topic = TOPIC

    try:
        lake = os.path.join(BASE_DIR, f"lake_latency_trial_{broker}")
        broker_dir = os.path.join(BASE_DIR, f"lake_latency_broker_{broker}")
        stats_out = os.path.join(BASE_DIR, f"lake_latency_producer_stats_{broker}.json")
        for d in (lake, broker_dir):
            shutil.rmtree(d, ignore_errors=True)
        os.environ["LOCAL_BROKER_DIR"] = broker_dir
        records = int(rate * (duration_s + warmup_s))

        sink = SinkConsumer(mode="effectively-once", batch_size=batch_size, batch_timeout=batch_timeout,
                            lake_dir=lake, state_db_path=os.path.join(lake, "sink_state.duckdb"),
                            topic=trial_topic, force_local=(broker == "local"),
                            require_kafka=(broker == "kafka"), bootstrap_servers=bootstrap_servers)
        t_sink = threading.Thread(target=lambda: sink.run(idle_exit_polls=None), daemon=True)
        t_sink.start()
        time.sleep(0.2)

        prod = start_producer_process(trial_topic, broker_dir, rate, records, stats_out,
                                      broker=broker, bootstrap_servers=bootstrap_servers)
        t_prod_start = time.time()
        max_lag = 0
        while prod.poll() is None:
            time.sleep(0.5)
            now_produced = topic_end_total_agnostic(trial_topic, broker=broker, broker_dir=broker_dir,
                                                    bootstrap_servers=bootstrap_servers, partitions=PARTITIONS)
            max_lag = max(max_lag, now_produced - sink.messages_polled)

        produced_total = topic_end_total_agnostic(trial_topic, broker=broker, broker_dir=broker_dir,
                                                  bootstrap_servers=bootstrap_servers, partitions=PARTITIONS)
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
    finally:
        if broker == "kafka":
            delete_kafka_topic(trial_topic, bootstrap_servers)


def run_latency_benchmark(batch_sizes: List[int], repetitions: int, rate: float, duration_s: float,
                          batch_timeout: float, max_new: Optional[int] = None,
                          broker: str = "local", bootstrap_servers: str = "localhost:9092") -> Optional[Dict[str, Any]]:
    partial_key = f"latency_{broker}" if broker != "local" else "latency"
    saved: Dict[str, Any] = load_partial(partial_key)
    budget = TrialBudget(max_new)
    results: Dict[str, Any] = {}
    for b in batch_sizes:
        print(f"\nB = {b:,} rows, T = {batch_timeout}s, rate = {rate:,.0f} ev/s ({repetitions} runs) [{broker}]", flush=True)
        trials = []
        for rep in range(1, repetitions + 1):
            key = f"{b}:{rep}"
            if key not in saved:
                if not budget.take():
                    print("Stopped after budget; run again to continue.")
                    return None
                saved[key] = run_single_latency_trial(b, rate, duration_s, batch_timeout, WARMUP_S,
                                                      broker=broker, bootstrap_servers=bootstrap_servers)
                save_partial(partial_key, saved)
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
    out_name = result_filename("latency_benchmark.json", broker=broker)
    write_result(out_name, {
        "parameters": {
            "producer_rate_events_per_s": rate, "duration_s": duration_s, "batch_timeout_s": batch_timeout,
            "repetitions": repetitions, "partitions": PARTITIONS, "warmup_s": WARMUP_S,
            "batch_size_is_per_partition": True,
            "spec_batch_sizes": [1000, 10000, 50000],
            "note": "Sizes outside the spec set are extra points to show where the timer takes over.",
        },
        "by_batch_size": results,
    }, force_local=(broker == "local"), require_kafka=(broker == "kafka"),
       bootstrap_servers=bootstrap_servers, broker=broker)
    clear_partial(partial_key)
    print(f"\nSaved results/{out_name}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Latency benchmark.")
    parser.add_argument("--broker", type=str, choices=["local", "kafka"], default="local",
                        help="Broker engine ('local' for disk emulator, 'kafka' for Redpanda/Kafka)")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=[250, 1000, 3000, 10000, 50000])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--rate", type=float, default=2000.0)
    parser.add_argument("--duration", type=float, default=40.0)
    parser.add_argument("--batch-timeout", type=float, default=10.0)
    parser.add_argument("--max-new", type=int, default=None)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()
    partial_key = f"latency_{args.broker}" if args.broker != "local" else "latency"
    if args.fresh:
        clear_partial(partial_key)
    run_latency_benchmark(args.batch_sizes, args.repetitions, args.rate, args.duration,
                          args.batch_timeout, args.max_new, broker=args.broker,
                          bootstrap_servers=args.bootstrap_servers)


if __name__ == "__main__":
    main()
