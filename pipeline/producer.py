"""Replays NYC Yellow Taxi Parquet files to Redpanda/Kafka.

Applies sanity checks, adds row_id and produce_ts, and streams
records at a target rate or max unthrottled throughput.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pyarrow.parquet as pq

from pipeline.broker import create_producer
from pipeline.validation import TripValidator

DEFAULT_DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "yellow_tripdata_2024-01.parquet",
)
DEFAULT_STATS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "results",
    "validation_rejects.json",
)


def serialize_val(val: Any) -> Any:
    """Converts PyArrow and datetime values to JSON-safe Python types."""
    if val is None:
        return None
    if isinstance(val, (datetime,)):
        return val.isoformat()
    if hasattr(val, "as_py"):
        return val.as_py()
    return val


def run_producer(
    source_path: str = DEFAULT_DATA_PATH,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "trips",
    target_rate: Optional[float] = None,
    max_records: Optional[int] = None,
    save_stats: Optional[str] = DEFAULT_STATS_PATH,
    force_local: bool = False,
) -> Dict[str, Any]:
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source parquet not found: {source_path}")

    producer = create_producer(bootstrap_servers=bootstrap_servers, force_local=force_local)
    validator = TripValidator(target_year=2024, target_month=1)

    parquet_file = pq.ParquetFile(source_path)
    total_file_rows = parquet_file.metadata.num_rows

    row_index = 0
    produced_count = 0
    start_time = time.perf_counter()
    last_rate_check_time = start_time
    last_produced_checkpoint = 0

    print(f"Reading {source_path} ({total_file_rows:,} rows in file)")
    print(f"Target rate: {f'{target_rate:,} ev/s' if target_rate else 'unthrottled'}")

    for record_batch in parquet_file.iter_batches(batch_size=10000):
        batch_dicts = record_batch.to_pylist()

        for raw_row in batch_dicts:
            current_row_id = row_index
            row_index += 1

            is_valid, _ = validator.validate_row(raw_row)
            if not is_valid:
                continue

            now_iso = datetime.now(timezone.utc).isoformat()
            now_epoch = time.time()

            payload = {
                "row_id": current_row_id,
                "produce_ts": now_iso,
                "produce_epoch_ms": int(now_epoch * 1000),
                "VendorID": serialize_val(raw_row.get("VendorID")),
                "tpep_pickup_datetime": serialize_val(raw_row.get("tpep_pickup_datetime")),
                "tpep_dropoff_datetime": serialize_val(raw_row.get("tpep_dropoff_datetime")),
                "passenger_count": serialize_val(raw_row.get("passenger_count")),
                "trip_distance": float(raw_row.get("trip_distance", 0.0)),
                "RatecodeID": serialize_val(raw_row.get("RatecodeID")),
                "store_and_fwd_flag": serialize_val(raw_row.get("store_and_fwd_flag")),
                "PULocationID": int(raw_row.get("PULocationID", 0)),
                "DOLocationID": int(raw_row.get("DOLocationID", 0)),
                "payment_type": serialize_val(raw_row.get("payment_type")),
                "fare_amount": float(raw_row.get("fare_amount", 0.0)),
                "extra": float(raw_row.get("extra", 0.0)),
                "mta_tax": float(raw_row.get("mta_tax", 0.0)),
                "tip_amount": float(raw_row.get("tip_amount", 0.0)),
                "tolls_amount": float(raw_row.get("tolls_amount", 0.0)),
                "improvement_surcharge": float(raw_row.get("improvement_surcharge", 0.0)),
                "total_amount": float(raw_row.get("total_amount", 0.0)),
                "congestion_surcharge": float(raw_row.get("congestion_surcharge", 0.0)),
                "Airport_fee": float(raw_row.get("Airport_fee", 0.0)),
            }

            # Key messages by pickup location to distribute traffic across partitions
            key_bytes = str(payload["PULocationID"]).encode("utf-8")
            val_bytes = json.dumps(payload).encode("utf-8")

            producer.produce(topic=topic, key=key_bytes, value=val_bytes)
            produced_count += 1

            if target_rate and target_rate > 0:
                if produced_count % 500 == 0:
                    producer.poll(0)
                    expected_time = produced_count / target_rate
                    elapsed = time.perf_counter() - start_time
                    delay = expected_time - elapsed
                    if delay > 0.001:
                        time.sleep(delay)
            elif produced_count % 5000 == 0:
                producer.poll(0)

            if max_records and produced_count >= max_records:
                break

        if max_records and produced_count >= max_records:
            break

        if produced_count - last_produced_checkpoint >= 100000:
            now = time.perf_counter()
            inst_rate = (produced_count - last_produced_checkpoint) / max(0.001, now - last_rate_check_time)
            print(f"Sent {produced_count:,} events ({inst_rate:,.0f} ev/s)")
            last_rate_check_time = now
            last_produced_checkpoint = produced_count

    producer.flush()
    total_time = max(0.001, time.perf_counter() - start_time)
    avg_rate = produced_count / total_time

    val_stats = validator.get_stats()
    summary = {
        "source_file": source_path,
        "total_source_rows_scanned": validator.total_processed,
        "accepted_and_produced": produced_count,
        "rejected_rows": validator.total_rejected,
        "reject_counts_by_rule": val_stats["reject_counts"],
        "elapsed_seconds": round(total_time, 3),
        "avg_events_per_second": round(avg_rate, 1),
    }

    print("\n--- Producer Summary ---")
    print(f"Scanned:  {summary['total_source_rows_scanned']:,} rows")
    print(f"Sent:     {summary['accepted_and_produced']:,} rows ({summary['avg_events_per_second']:,} ev/s)")
    print(f"Rejected: {summary['rejected_rows']:,} rows")
    for rule, count in summary["reject_counts_by_rule"].items():
        print(f"  {rule}: {count:,}")

    if save_stats:
        os.makedirs(os.path.dirname(save_stats), exist_ok=True)
        with open(save_stats, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Replay NYC TLC Yellow Taxi trips to streaming broker.")
    parser.add_argument("--source", type=str, default=DEFAULT_DATA_PATH, help="Path to NYC TLC Parquet file")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Broker bootstrap servers")
    parser.add_argument("--topic", type=str, default="trips", help="Target topic")
    parser.add_argument("--rate", type=float, default=None, help="Target event rate (events/s); omit for max")
    parser.add_argument("--max-records", type=int, default=None, help="Stop after producing N accepted records")
    parser.add_argument("--stats-out", type=str, default=DEFAULT_STATS_PATH, help="Path to save rejection statistics")
    parser.add_argument("--force-local", action="store_true", help="Force local broker emulator")
    args = parser.parse_args()

    run_producer(
        source_path=args.source,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        target_rate=args.rate,
        max_records=args.max_records,
        save_stats=args.stats_out,
        force_local=args.force_local,
    )


if __name__ == "__main__":
    main()
