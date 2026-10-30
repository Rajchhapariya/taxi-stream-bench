"""Small-files problem: query time and file count before and after compaction.

The sink writes micro-batches of B rows per partition, so the lake fills with small files. Row
counts are measured from the lake itself (the first version reported the requested count, and the
sink had in fact written about a twelfth of it). Compaction must leave the row count and the set of
distinct row_ids unchanged, and the run fails if it does not. Each query is timed 5 times after one
warm-up run and the median is kept.
"""

import argparse
import os
import shutil
import statistics
import sys
import time
import uuid
from typing import Any, Dict, List

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import duckdb

from bench.common import BASE_DIR, DATA_PATH, lake_stats, result_filename, write_result
from pipeline.broker import BrokerUnavailableError, create_kafka_topic, delete_kafka_topic, kafka_usable
from pipeline.compaction import compact_lake
from pipeline.producer import run_producer
from pipeline.queries import query_avg_fare_per_distance, query_hourly_trips, query_top_10_pairs
from pipeline.sink import SinkConsumer

TOPIC = "trips_compact"
QUERIES = [
    ("hourly_trips_per_zone", query_hourly_trips),
    ("avg_fare_per_distance_bucket", query_avg_fare_per_distance),
    ("top_10_zone_pairs", query_top_10_pairs),
]


def time_queries(lake: str) -> Dict[str, float]:
    con = duckdb.connect()
    out = {}
    for name, fn in QUERIES:
        fn(lake, con)  # warm-up
        runs = [fn(lake, con)[1] * 1000.0 for _ in range(5)]
        out[name] = round(statistics.median(runs), 2)
    con.close()
    return out


def run_compaction_trial(records: int, micro_batch: int, broker: str = "local",
                         bootstrap_servers: str = "localhost:9092") -> Dict[str, Any]:
    if broker == "kafka":
        if not kafka_usable(bootstrap_servers):
            raise BrokerUnavailableError(f"Broker unavailable at {bootstrap_servers} for kafka-mode compaction benchmark")
        trial_topic = f"trips_compact_{uuid.uuid4().hex[:6]}"
        create_kafka_topic(trial_topic, bootstrap_servers, num_partitions=6)
    else:
        trial_topic = TOPIC

    try:
        lake = os.path.join(BASE_DIR, f"lake_compaction_bench_{broker}")
        broker_dir = os.path.join(BASE_DIR, f"lake_compaction_broker_{broker}")
        for d in (lake, broker_dir):
            shutil.rmtree(d, ignore_errors=True)
        os.environ["LOCAL_BROKER_DIR"] = broker_dir

        prod = run_producer(source_path=DATA_PATH, topic=trial_topic, max_records=records,
                            force_local=(broker == "local"), require_kafka=(broker == "kafka"),
                            bootstrap_servers=bootstrap_servers, save_stats=None)
        sink = SinkConsumer(mode="effectively-once", batch_size=micro_batch, batch_timeout=5.0,
                            lake_dir=lake, state_db_path=os.path.join(lake, "sink_state.duckdb"),
                            topic=trial_topic, force_local=(broker == "local"),
                            require_kafka=(broker == "kafka"), bootstrap_servers=bootstrap_servers)
        sink.run()

        before = lake_stats(lake)
        if before["rows"] != prod["accepted_and_produced"] or before["unique_row_ids"] != before["rows"]:
            raise RuntimeError(f"Lake does not hold the produced rows before compaction: {before} vs {prod['accepted_and_produced']}")
        q_before = time_queries(lake)

        comp = compact_lake(lake_dir=lake, state_db_path=os.path.join(lake, "sink_state.duckdb"))

        after = lake_stats(lake)
        if after["rows"] != before["rows"] or after["unique_row_ids"] != before["unique_row_ids"]:
            raise RuntimeError(f"Compaction changed the data: before {before}, after {after}")
        q_after = time_queries(lake)

        return {
            "rows_produced": prod["accepted_and_produced"],
            "rows_before": before["rows"],
            "rows_after": after["rows"],
            "files_before": before["files"],
            "files_after": after["files"],
            "size_before_mb": round(before["bytes"] / 1024 / 1024, 3),
            "size_after_mb": round(after["bytes"] / 1024 / 1024, 3),
            "compaction_time_sec": comp["compaction_time_sec"],
            "query_ms_before": q_before,
            "query_ms_after": q_after,
        }
    finally:
        if broker == "kafka":
            delete_kafka_topic(trial_topic, bootstrap_servers)


def run_compaction_benchmark(records: int, micro_batch: int, repetitions: int,
                             broker: str = "local", bootstrap_servers: str = "localhost:9092") -> Dict[str, Any]:
    trials: List[Dict[str, Any]] = []
    for rep in range(1, repetitions + 1):
        t = run_compaction_trial(records, micro_batch, broker=broker, bootstrap_servers=bootstrap_servers)
        print(f"run {rep}: {t['rows_before']:,} rows, {t['files_before']} -> {t['files_after']} files, "
              f"{t['size_before_mb']} -> {t['size_after_mb']} MB, "
              f"queries {sum(t['query_ms_before'].values()):.1f} -> {sum(t['query_ms_after'].values()):.1f} ms", flush=True)
        trials.append(t)
    med = lambda f: round(statistics.median(f(t) for t in trials), 2)
    out_name = result_filename("compaction_benchmark.json", broker=broker)
    return write_result(out_name, {
        "parameters": {"records": records, "micro_batch_rows_per_partition": micro_batch,
                       "repetitions": repetitions, "query_runs_per_timing": 5},
        "median": {
            "files_before": med(lambda t: t["files_before"]),
            "files_after": med(lambda t: t["files_after"]),
            "query_ms_before": {n: med(lambda t, n=n: t["query_ms_before"][n]) for n, _ in QUERIES},
            "query_ms_after": {n: med(lambda t, n=n: t["query_ms_after"][n]) for n, _ in QUERIES},
            "compaction_time_sec": med(lambda t: t["compaction_time_sec"]),
        },
        "trials": trials,
    }, force_local=(broker == "local"), require_kafka=(broker == "kafka"),
       bootstrap_servers=bootstrap_servers, broker=broker)


def main():
    parser = argparse.ArgumentParser(description="Compaction benchmark.")
    parser.add_argument("--broker", type=str, choices=["local", "kafka"], default="local",
                        help="Broker engine ('local' for disk emulator, 'kafka' for Redpanda/Kafka)")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--records", type=int, default=40000)
    parser.add_argument("--micro-batch", type=int, default=50)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    run_compaction_benchmark(args.records, args.micro_batch, args.repetitions,
                             broker=args.broker, bootstrap_servers=args.bootstrap_servers)


if __name__ == "__main__":
    main()
