"""Parquet lake sink consumer.

Supports two delivery models:
- Mode A (at-least-once): writes Parquet files, then commits offsets to Kafka.
  Crashes in the gap cause duplicate rows.
- Mode B (effectively-once): idempotent file naming with offset ranges, atomic
  rename from temp files, and offset tracking in DuckDB.
"""

import argparse
import glob
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.broker import TopicPartition, broker_label, create_consumer

DEFAULT_LAKE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "lake",
)
DEFAULT_STATE_DB = os.path.join(DEFAULT_LAKE_DIR, "sink_state.duckdb")

PARQUET_SCHEMA = pa.schema([
    ("row_id", pa.int64()),
    ("produce_ts", pa.string()),
    ("produce_epoch_ms", pa.int64()),
    ("sink_commit_epoch_ms", pa.int64()),
    ("VendorID", pa.int32()),
    ("tpep_pickup_datetime", pa.string()),
    ("tpep_dropoff_datetime", pa.string()),
    ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()),
    ("RatecodeID", pa.int64()),
    ("store_and_fwd_flag", pa.string()),
    ("PULocationID", pa.int32()),
    ("DOLocationID", pa.int32()),
    ("payment_type", pa.int64()),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    ("congestion_surcharge", pa.float64()),
    ("Airport_fee", pa.float64()),
])


def clean_orphan_tmp_files(lake_dir: str):
    """Removes orphan .tmp files left by killed or crashed sink processes."""
    for root, _, files in os.walk(lake_dir):
        for f in files:
            if f.endswith(".tmp"):
                try:
                    os.remove(os.path.join(root, f))
                except OSError:
                    pass


class SinkConsumer:
    def __init__(
        self,
        mode: str = "effectively-once",
        batch_size: int = 1000,
        batch_timeout: float = 1.0,
        lake_dir: str = DEFAULT_LAKE_DIR,
        state_db_path: str = DEFAULT_STATE_DB,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "taxi-sink-group",
        topic: str = "trips",
        force_local: bool = False,
        crash_after_rename: bool = False,
        crash_before_rename: bool = False,
        num_partitions: int = 6,
        commit_delay: float = 0.02,
        crash_prob: float = 0.0,
        crash_seed: Optional[int] = None,
        reconcile_orphans: bool = True,
        require_kafka: bool = False,
    ):
        if mode not in ("at-least-once", "effectively-once"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'at-least-once' or 'effectively-once'.")

        self.mode = mode
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.lake_dir = os.path.abspath(lake_dir)
        self.state_db_path = os.path.abspath(state_db_path)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.force_local = force_local
        self.num_partitions = num_partitions
        self.crash_after_rename = crash_after_rename
        self.crash_before_rename = crash_before_rename
        self.commit_delay = commit_delay
        self.crash_prob = crash_prob
        self._rng = random.Random(crash_seed)
        self.reconcile_orphans = reconcile_orphans
        self.broker = broker_label(bootstrap_servers, force_local, require_kafka)
        self.reconciled_files = 0
        self._crashed = False

        os.makedirs(self.lake_dir, exist_ok=True)
        self.running = True

        self.con = None
        if self.mode == "effectively-once":
            clean_orphan_tmp_files(self.lake_dir)
            self._init_duckdb_state()
            if self.reconcile_orphans:
                self._reconcile_unmanifested_files()

        self.consumer = create_consumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            force_local=self.force_local,
            require_kafka=require_kafka,
            num_partitions=self.num_partitions,
        )
        self.consumer.subscribe([self.topic])

        if self.mode == "effectively-once":
            self._resume_from_duckdb_offsets()

        # Buffers and offset bounds per partition
        self.buffers: Dict[int, List[Dict[str, Any]]] = {p: [] for p in range(num_partitions)}
        self.start_offsets: Dict[int, Optional[int]] = {p: None for p in range(num_partitions)}
        self.end_offsets: Dict[int, Optional[int]] = {p: None for p in range(num_partitions)}
        self.last_flush_times: Dict[int, float] = {p: time.time() for p in range(num_partitions)}

        self.total_rows_committed = 0
        self.total_batches_committed = 0
        self.messages_polled = 0
        self.latencies_ms: List[float] = []
        # One entry per flushed batch: (partition, reason, per-row latencies in ms).
        # reason is "size", "timeout" or "final" (end-of-run flush of a partial batch).
        self.batch_latencies: List[Tuple[int, str, List[float], float]] = []  # partition, reason, latencies, wall-clock commit time

    def _init_duckdb_state(self):
        # On Windows, terminating a child process can briefly hold file locks.
        # Retry connection with backoff.
        connected = False
        for _ in range(15):
            try:
                self.con = duckdb.connect(self.state_db_path)
                connected = True
                break
            except duckdb.IOException:
                time.sleep(0.1)

        if not connected:
            self.con = duckdb.connect(self.state_db_path)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS lake_manifest (
                topic VARCHAR,
                partition INTEGER,
                start_offset BIGINT,
                end_offset BIGINT,
                file_path VARCHAR,
                row_count BIGINT,
                committed_at TIMESTAMP,
                PRIMARY KEY (topic, partition, start_offset, end_offset)
            );
        """)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS partition_offsets (
                topic VARCHAR,
                partition INTEGER,
                max_committed_offset BIGINT,
                PRIMARY KEY (topic, partition)
            );
        """)

    def _reconcile_unmanifested_files(self):
        """Removes sink-named Parquet files that the manifest does not list.

        A kill between os.replace and the DuckDB commit leaves a finished file whose offsets
        are not recorded. The restarted sink re-reads those offsets, and because batch
        boundaries depend on timing the new file can have a different offset range, so name
        based idempotence alone would keep both files. Deleting the unrecorded file makes the
        manifest the single source of truth for what is in the lake.
        """
        known = {
            os.path.normpath(r[0])
            for r in self.con.execute("SELECT file_path FROM lake_manifest").fetchall()
        }
        pattern = os.path.join(self.lake_dir, "**", "trips-p*-o*-o*.parquet")
        for path in glob.glob(pattern, recursive=True):
            rel = os.path.normpath(os.path.relpath(path, self.lake_dir))
            if rel not in known:
                try:
                    os.remove(path)
                    self.reconciled_files += 1
                except OSError:
                    pass

    def _crash(self, code: int):
        """Simulates a process crash: no flush, no consumer close, no DuckDB close."""
        self._crashed = True
        raise SystemExit(code)

    def _resume_from_duckdb_offsets(self):
        """Resumes consuming from offsets stored in DuckDB rather than Kafka committed offsets."""
        res = self.con.execute("""
            SELECT partition, max_committed_offset
            FROM partition_offsets
            WHERE topic = ?
        """, [self.topic]).fetchall()

        if res:
            for p, max_off in res:
                resume_off = max_off + 1
                tp = TopicPartition(self.topic, p, resume_off)
                self.consumer.seek(tp)

    def _is_batch_in_manifest(self, partition: int, start_offset: int, end_offset: int) -> bool:
        if self.mode != "effectively-once":
            return False
        row = self.con.execute("""
            SELECT 1 FROM lake_manifest
            WHERE topic = ? AND partition = ? AND start_offset = ? AND end_offset = ?
        """, [self.topic, partition, start_offset, end_offset]).fetchone()
        return row is not None

    def _records_to_arrow_table(self, records: List[Dict[str, Any]], commit_ms: int) -> pa.Table:
        columns = {col.name: [] for col in PARQUET_SCHEMA}
        for rec in records:
            columns["row_id"].append(rec["row_id"])
            columns["produce_ts"].append(rec.get("produce_ts", ""))
            columns["produce_epoch_ms"].append(rec.get("produce_epoch_ms", 0))
            columns["sink_commit_epoch_ms"].append(commit_ms)
            columns["VendorID"].append(rec.get("VendorID"))
            columns["tpep_pickup_datetime"].append(str(rec.get("tpep_pickup_datetime", "")))
            columns["tpep_dropoff_datetime"].append(str(rec.get("tpep_dropoff_datetime", "")))
            columns["passenger_count"].append(rec.get("passenger_count"))
            columns["trip_distance"].append(float(rec.get("trip_distance", 0.0)))
            columns["RatecodeID"].append(rec.get("RatecodeID"))
            columns["store_and_fwd_flag"].append(rec.get("store_and_fwd_flag"))
            columns["PULocationID"].append(int(rec.get("PULocationID", 0)))
            columns["DOLocationID"].append(int(rec.get("DOLocationID", 0)))
            columns["payment_type"].append(rec.get("payment_type"))
            columns["fare_amount"].append(float(rec.get("fare_amount", 0.0)))
            columns["extra"].append(float(rec.get("extra", 0.0)))
            columns["mta_tax"].append(float(rec.get("mta_tax", 0.0)))
            columns["tip_amount"].append(float(rec.get("tip_amount", 0.0)))
            columns["tolls_amount"].append(float(rec.get("tolls_amount", 0.0)))
            columns["improvement_surcharge"].append(float(rec.get("improvement_surcharge", 0.0)))
            columns["total_amount"].append(float(rec.get("total_amount", 0.0)))
            columns["congestion_surcharge"].append(float(rec.get("congestion_surcharge", 0.0)))
            columns["Airport_fee"].append(float(rec.get("Airport_fee", 0.0)))

        return pa.Table.from_pydict(columns, schema=PARQUET_SCHEMA)

    def _flush_partition(self, partition: int, reason: str = "size"):
        records = self.buffers[partition]
        if not records:
            return

        start_offset = self.start_offsets[partition]
        end_offset = self.end_offsets[partition]
        row_count = len(records)
        now_epoch_ms = int(time.time() * 1000)

        pickup_str = records[0].get("tpep_pickup_datetime") or ""
        date_folder = pickup_str[:10] if len(pickup_str) >= 10 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dest_dir = os.path.join(self.lake_dir, f"date={date_folder}")
        os.makedirs(dest_dir, exist_ok=True)

        table = self._records_to_arrow_table(records, now_epoch_ms)

        batch_lat = [max(0.0, now_epoch_ms - rec.get("produce_epoch_ms", now_epoch_ms)) for rec in records]
        self.latencies_ms.extend(batch_lat)
        self.batch_latencies.append((partition, reason, batch_lat, time.time()))

        if self.mode == "at-least-once":
            # Mode A: write file directly, then commit offset to Kafka
            batch_uuid = uuid.uuid4().hex[:8]
            final_filename = f"trips-p{partition}-b{batch_uuid}.parquet"
            final_path = os.path.join(dest_dir, final_filename)

            pq.write_table(table, final_path, compression="snappy")

            # Simulated crash after file write but before offset commit
            if self.crash_after_rename or (self.crash_prob > 0 and self._rng.random() < self.crash_prob):
                self._crash(42)

            if self.commit_delay > 0:
                time.sleep(self.commit_delay)

            tp = TopicPartition(self.topic, partition, end_offset + 1)
            self.consumer.commit([tp])

        else:
            # Mode B: idempotent offset naming, atomic rename, and DuckDB manifest
            if self._is_batch_in_manifest(partition, start_offset, end_offset):
                self.buffers[partition] = []
                self.start_offsets[partition] = None
                self.end_offsets[partition] = None
                self.last_flush_times[partition] = time.time()
                return

            final_filename = f"trips-p{partition}-o{start_offset}-o{end_offset}.parquet"
            final_path = os.path.join(dest_dir, final_filename)
            temp_path = os.path.join(dest_dir, f".{final_filename}.tmp")

            pq.write_table(table, temp_path, compression="snappy")

            if self.crash_before_rename:
                self._crash(43)

            # Atomic rename from staging to final file
            os.replace(temp_path, final_path)

            if self.crash_after_rename or (self.crash_prob > 0 and self._rng.random() < self.crash_prob):
                self._crash(44)

            # Record offset and manifest in one DuckDB transaction
            rel_file_path = os.path.relpath(final_path, self.lake_dir)
            self.con.execute("BEGIN TRANSACTION;")
            try:
                self.con.execute("""
                    INSERT OR REPLACE INTO lake_manifest
                    (topic, partition, start_offset, end_offset, file_path, row_count, committed_at)
                    VALUES (?, ?, ?, ?, ?, ?, current_timestamp);
                """, [self.topic, partition, start_offset, end_offset, rel_file_path, row_count])

                self.con.execute("""
                    INSERT OR REPLACE INTO partition_offsets (topic, partition, max_committed_offset)
                    VALUES (?, ?, ?);
                """, [self.topic, partition, end_offset])
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

        self.total_rows_committed += row_count
        self.total_batches_committed += 1

        self.buffers[partition] = []
        self.start_offsets[partition] = None
        self.end_offsets[partition] = None
        self.last_flush_times[partition] = time.time()

    def run(
        self,
        max_messages: Optional[int] = None,
        max_seconds: Optional[float] = None,
        idle_exit_polls: Optional[int] = 20,
    ) -> Dict[str, Any]:
        start_time = time.time()
        messages_polled = 0
        empty_polls = 0
        last_log_time = start_time

        try:
            while self.running:
                msg = self.consumer.poll(timeout=0.05)

                if msg is not None and msg.error() is None:
                    empty_polls = 0
                    p = msg.partition()
                    off = msg.offset()
                    val = json.loads(msg.value().decode("utf-8"))

                    if not self.buffers[p]:
                        self.start_offsets[p] = off
                    self.end_offsets[p] = off
                    self.buffers[p].append(val)
                    messages_polled += 1
                    self.messages_polled = messages_polled

                    if len(self.buffers[p]) >= self.batch_size:
                        self._flush_partition(p, "size")

                    if max_messages and messages_polled >= max_messages:
                        for part in range(self.num_partitions):
                            if self.buffers[part]:
                                self._flush_partition(part, "final")
                        break
                else:
                    empty_polls += 1
                    if idle_exit_polls is not None and empty_polls >= idle_exit_polls:
                        for part in range(self.num_partitions):
                            if self.buffers[part]:
                                self._flush_partition(part, "final")
                        break

                now = time.time()
                for p in range(self.num_partitions):
                    if self.buffers[p] and (now - self.last_flush_times[p] >= self.batch_timeout):
                        self._flush_partition(p, "timeout")

                if now - last_log_time >= 5.0:
                    print(f"Sink [{self.mode}]: {self.total_rows_committed:,} rows written ({self.total_batches_committed:,} files)")
                    last_log_time = now

                if max_messages and self.total_rows_committed >= max_messages:
                    break
                if max_seconds and (now - start_time) >= max_seconds:
                    break

        except KeyboardInterrupt:
            pass
        finally:
            # After a simulated crash nothing is flushed or closed, as with a killed process.
            if not self._crashed:
                for p in range(self.num_partitions):
                    if self.buffers[p]:
                        self._flush_partition(p, "final")
                self.consumer.close()
                if self.con:
                    self.con.close()

        elapsed = max(0.001, time.time() - start_time)
        return {
            "mode": self.mode,
            "broker": self.broker,
            "reconciled_files": self.reconciled_files,
            "messages_polled": self.messages_polled,
            "total_rows_committed": self.total_rows_committed,
            "total_batches_committed": self.total_batches_committed,
            "elapsed_seconds": round(elapsed, 3),
            "throughput_rows_per_sec": round(self.total_rows_committed / elapsed, 1),
            "latencies_count": len(self.latencies_ms),
        }


def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Parquet Lake Sink Consumer")
    parser.add_argument("--mode", type=str, choices=["at-least-once", "effectively-once"], default="effectively-once")
    parser.add_argument("-B", "--batch-size", type=int, default=1000, help="Micro-batch size in rows")
    parser.add_argument("-T", "--batch-timeout", type=float, default=1.0, help="Batch timeout in seconds")
    parser.add_argument("--lake-dir", type=str, default=DEFAULT_LAKE_DIR, help="Target Parquet lake directory")
    parser.add_argument("--state-db", type=str, default=DEFAULT_STATE_DB, help="DuckDB manifest path")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Broker bootstrap address")
    parser.add_argument("--group-id", type=str, default="taxi-sink-group", help="Consumer group id")
    parser.add_argument("--topic", type=str, default="trips", help="Kafka topic")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after committing N messages")
    parser.add_argument("--max-seconds", type=float, default=None, help="Stop after N seconds")
    parser.add_argument("--force-local", action="store_true", help="Force local broker emulator")
    parser.add_argument("--commit-delay", type=float, default=0.02, help="Commit network RPC delay window")
    parser.add_argument("--crash-prob", type=float, default=0.0, help="Simulated crash probability at commit boundary")
    parser.add_argument("--crash-after-rename", action="store_true", help="Crash injection hook")
    parser.add_argument("--crash-before-rename", action="store_true", help="Crash injection hook")
    parser.add_argument("--crash-seed", type=int, default=None, help="Seed for the crash-probability generator")
    parser.add_argument("--no-reconcile", action="store_true",
                        help="Mode B only: skip deleting Parquet files the manifest does not list")
    parser.add_argument("--require-kafka", action="store_true",
                        help="Fail instead of falling back to the local emulator")
    args = parser.parse_args()

    sink = SinkConsumer(
        mode=args.mode,
        batch_size=args.batch_size,
        batch_timeout=args.batch_timeout,
        lake_dir=args.lake_dir,
        state_db_path=args.state_db,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        topic=args.topic,
        force_local=args.force_local,
        crash_after_rename=args.crash_after_rename,
        crash_before_rename=args.crash_before_rename,
        commit_delay=args.commit_delay,
        crash_prob=args.crash_prob,
        crash_seed=args.crash_seed,
        reconcile_orphans=not args.no_reconcile,
        require_kafka=args.require_kafka,
    )
    sink.run(max_messages=args.max_messages, max_seconds=args.max_seconds)


if __name__ == "__main__":
    main()
