"""Compaction job for the streaming Parquet lake.

Consolidates small micro-batch files within each date partition
into single Parquet files with 100k-row row groups and updates DuckDB state.
"""

import argparse
import glob
import os
import time
from typing import Any, Dict

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_LAKE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "lake",
)
DEFAULT_STATE_DB = os.path.join(DEFAULT_LAKE_DIR, "sink_state.duckdb")


def compact_lake(lake_dir: str = DEFAULT_LAKE_DIR, state_db_path: str = DEFAULT_STATE_DB) -> Dict[str, Any]:
    lake_dir = os.path.abspath(lake_dir)
    state_db_path = os.path.abspath(state_db_path)

    date_dirs = [d for d in glob.glob(os.path.join(lake_dir, "date=*")) if os.path.isdir(d)]
    if not date_dirs:
        return {"error": "No date partition directories found to compact", "compacted_files": 0}

    initial_files = len(glob.glob(os.path.join(lake_dir, "**", "*.parquet"), recursive=True))
    total_rows_compacted = 0
    t0 = time.perf_counter()

    con = None
    if os.path.exists(state_db_path):
        con = duckdb.connect(state_db_path)

    for date_dir in date_dirs:
        parquet_files = [
            f for f in glob.glob(os.path.join(date_dir, "*.parquet"))
            if not os.path.basename(f).startswith("compacted-")
        ]
        if len(parquet_files) <= 1:
            continue

        print(f"Compacting {len(parquet_files)} files in {os.path.basename(date_dir)}...")

        tables = []
        for pf in parquet_files:
            try:
                tables.append(pq.read_table(pf))
            except Exception as e:
                print(f"Skipping corrupt or unreadable file {pf}: {e}")

        if not tables:
            continue

        consolidated = pa.concat_tables(tables)
        total_rows_compacted += consolidated.num_rows

        compacted_filename = "compacted-0001.parquet"
        temp_compacted = os.path.join(date_dir, f".{compacted_filename}.tmp")
        final_compacted = os.path.join(date_dir, compacted_filename)

        pq.write_table(consolidated, temp_compacted, compression="snappy", row_group_size=100000)
        os.replace(temp_compacted, final_compacted)

        for pf in parquet_files:
            try:
                os.remove(pf)
            except OSError:
                pass

        if con:
            rel_compacted = os.path.relpath(final_compacted, lake_dir)
            con.execute("BEGIN TRANSACTION;")
            con.execute("""
                INSERT OR REPLACE INTO lake_manifest
                (topic, partition, start_offset, end_offset, file_path, row_count, committed_at)
                VALUES ('trips', -1, 0, ?, ?, ?, current_timestamp);
            """, [consolidated.num_rows, rel_compacted, consolidated.num_rows])
            con.execute("COMMIT;")

    if con:
        con.close()

    elapsed = time.perf_counter() - t0
    final_files = len(glob.glob(os.path.join(lake_dir, "**", "*.parquet"), recursive=True))

    result = {
        "initial_files": initial_files,
        "final_files": final_files,
        "files_reduced": initial_files - final_files,
        "total_rows_compacted": total_rows_compacted,
        "compaction_time_sec": round(elapsed, 3),
    }

    print(f"Compaction done: reduced {initial_files} files to {final_files} in {result['compaction_time_sec']}s")
    return result


def main():
    parser = argparse.ArgumentParser(description="Compact small Parquet files in the lake.")
    parser.add_argument("--lake-dir", type=str, default=DEFAULT_LAKE_DIR, help="Lake directory to compact")
    parser.add_argument("--state-db", type=str, default=DEFAULT_STATE_DB, help="DuckDB state database path")
    args = parser.parse_args()

    compact_lake(lake_dir=args.lake_dir, state_db_path=args.state_db)


if __name__ == "__main__":
    main()
