"""Sustained throughput and consumer lag versus producer target rate.

The producer runs in its own process. The sink runs in this process and is stopped the moment the
producer finishes, so the numbers describe what the sink sustains while data arrives, with lag
sampled every 0.5 s (rows on the topic minus rows the sink has read). The sink rate counts rows read; rows_committed_at_producer_end shows how many of them had reached a Parquet file, which trails by up to one batch per partition. The first 20 percent of the
production interval is excluded from the rate as warm-up.

A target above what the single-threaded Python sink can absorb shows up as growing lag, not as a
higher sustained rate. Note that against the local emulator the read path (one index seek and one
log seek per message) is part of what is being measured.
"""

import argparse
import os
import shutil
import statistics
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from bench.common import (BASE_DIR, TrialBudget, clear_partial, load_partial, save_partial,
                          start_producer_process, topic_end_total, write_result)
from pipeline.sink import SinkConsumer

TOPIC = "trips_tp"
SAMPLE_S = 0.5
WARMUP_FRACTION = 0.2


def run_single_throughput_trial(target_rate: float, num_records: int, batch_size: int, batch_timeout: float) -> Dict[str, Any]:
    lake = os.path.join(BASE_DIR, "lake_throughput_trial")
    broker_dir = os.path.join(BASE_DIR, "lake_throughput_broker")
    stats_out = os.path.join(BASE_DIR, "lake_throughput_producer_stats.json")
    for d in (lake, broker_dir):
        shutil.rmtree(d, ignore_errors=True)
    os.environ["LOCAL_BROKER_DIR"] = broker_dir

    sink = SinkConsumer(mode="effectively-once", batch_size=batch_size, batch_timeout=batch_timeout,
                        lake_dir=lake, state_db_path=os.path.join(lake, "sink_state.duckdb"),
                        topic=TOPIC, force_local=True)
    t_sink = threading.Thread(target=lambda: sink.run(idle_exit_polls=None), daemon=True)
    t_sink.start()
    time.sleep(0.2)

    prod = start_producer_process(TOPIC, broker_dir, target_rate if target_rate > 0 else None, num_records, stats_out)
    t0 = time.perf_counter()
    samples: List[Tuple[float, int, int]] = []  # (t, produced, consumed)
    while prod.poll() is None:
        time.sleep(SAMPLE_S)
        samples.append((time.perf_counter() - t0, topic_end_total(broker_dir, TOPIC), sink.messages_polled))
    produced_end = topic_end_total(broker_dir, TOPIC)
    consumed_end = sink.messages_polled
    committed_end = sink.total_rows_committed
    t_end = time.perf_counter() - t0
    samples.append((t_end, produced_end, consumed_end))
    sink.running = False
    t_sink.join(timeout=30)

    first = next((s for s in samples if s[1] > 0), samples[0])
    span = max(0.001, t_end - first[0])
    w_start = first[0] + WARMUP_FRACTION * span
    w0 = min((s for s in samples if s[0] >= w_start), key=lambda s: s[0], default=first)
    window = max(0.001, t_end - w0[0])
    # Both rates are measured over the same window, after the warm-up fraction.
    produce_rate = (produced_end - w0[1]) / window
    sink_rate = (consumed_end - w0[2]) / window
    lags = [s[1] - s[2] for s in samples]

    return {
        "target_rate": target_rate,
        "rows_produced": produced_end,
        "achieved_produce_rate": round(produce_rate, 1),
        "sustained_sink_rate": round(sink_rate, 1),  # rows read by the sink per second
        "rows_committed_at_producer_end": committed_end,
        "peak_lag_rows": max(lags),
        "lag_at_producer_end_rows": lags[-1],
        "production_seconds": round(span, 1),
    }


def run_throughput_benchmark(rates: List[float], repetitions: int, batch_size: int, batch_timeout: float,
                             records_for_rate: Dict[float, int], max_new: Optional[int] = None) -> Optional[Dict[str, Any]]:
    saved: Dict[str, Any] = load_partial("throughput")
    budget = TrialBudget(max_new)
    results: Dict[str, Any] = {}
    for rate in rates:
        label = "MAX" if rate == 0.0 else f"{int(rate)}"
        n = records_for_rate[rate]
        print(f"\nTarget {label} ev/s, {n:,} records ({repetitions} runs)", flush=True)
        trials = []
        for rep in range(1, repetitions + 1):
            key = f"{label}:{rep}"
            if key not in saved:
                if not budget.take():
                    print("Stopped after budget; run again to continue.")
                    return None
                saved[key] = run_single_throughput_trial(rate, n, batch_size, batch_timeout)
                save_partial("throughput", saved)
            t = saved[key]
            print(f"  run {rep}: produce {t['achieved_produce_rate']} ev/s, sink {t['sustained_sink_rate']} ev/s, "
                  f"peak lag {t['peak_lag_rows']:,}", flush=True)
            trials.append(t)
        sink_rates = [t["sustained_sink_rate"] for t in trials]
        results[label] = {
            "target_rate": rate,
            "median_sustained_sink_rate": round(statistics.median(sink_rates), 1),
            "min_sustained_sink_rate": min(sink_rates),
            "max_sustained_sink_rate": max(sink_rates),
            "median_achieved_produce_rate": round(statistics.median(t["achieved_produce_rate"] for t in trials), 1),
            "median_peak_lag_rows": int(statistics.median(t["peak_lag_rows"] for t in trials)),
            "trials": trials,
        }
    write_result("throughput_benchmark.json", {
        "parameters": {"batch_size": batch_size, "batch_timeout_s": batch_timeout, "sample_interval_s": SAMPLE_S,
                       "warmup_fraction": WARMUP_FRACTION, "repetitions": repetitions,
                       "records_per_target": {("MAX" if k == 0.0 else str(int(k))): v for k, v in records_for_rate.items()}},
        "by_target_rate": results,
    }, force_local=True)
    clear_partial("throughput")
    return results


def main():
    parser = argparse.ArgumentParser(description="Throughput benchmark.")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--batch-timeout", type=float, default=1.0)
    parser.add_argument("--max-new", type=int, default=None)
    parser.add_argument("--fresh", action="store_true")
    args = parser.parse_args()
    if args.fresh:
        clear_partial("throughput")
    rates = [5000.0, 20000.0, 50000.0, 0.0]
    records = {5000.0: 100000, 20000.0: 300000, 50000.0: 300000, 0.0: 300000}
    run_throughput_benchmark(rates, args.repetitions, args.batch_size, args.batch_timeout, records, args.max_new)


if __name__ == "__main__":
    main()
