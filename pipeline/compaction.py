"""Compaction job for the streaming Parquet lake.

Merges the small micro-batch files inside each date partition into one new Parquet file with
100k-row row groups.

Safety rules, each of which was missing from the first version:
- The output name is unique, so a second compaction never overwrites an earlier compacted file.
- Only files that were read successfully and written into the output are deleted. A file that
  cannot be read is left in place and counted.
- The output row count is checked against the sum of the inputs before anything is deleted.
- The sink manifest (lake_manifest) is not edited. Compactions are recorded in a separate
  compaction_log table.

Run it while the sink is stopped. It is not safe against a concurrent writer, and a crash after
the new file is renamed but before the inputs are deleted leaves both on disk, so readers would
see duplicate rows until the job is run again by hand.
"""

import argparse
import glob
import os
import time
from typing import Any, Dict, List

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_LAKE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "lake",
)
DEFAULT_STATE_DB = os.path.join(DEFAULT_LAKE_DIR, "sink_state.duckdb")


def _next_compacted_path(date_dir: str) -> str:
    n = len(glob.glob(os.path.join(date_dir, "compacted-*.parquet"))) + 1
    while True:
        path = os.path.join(date_dir, f"compacted-{n:04d}.parquet")
        if not os.path.exists(path):
            return path
        n += 1


def compact_lake(lake_dir: str = DEFAULT_LAKE_DIR, state_db_path: str = DEFAULT_STATE_DB) -> Dict[str, Any]:
    lake_dir = os.path.abspath(lake_dir)
    state_db_path = os.path.abspath(state_db_path)

    date_dirs = [d for d in glob.glob(os.path.join(lake_dir, "date=*")) if os.path.isdir(d)]
    if not date_dirs:
        return {"error": "No date partition directories found to compact", "compacted_files": 0}

    initial_files = len(glob.glob(os.path.join(lake_dir, "**", "*.parquet"), recursive=True))
    rows_in = 0
    rows_out = 0
    skipped_unreadable: List[str] = []
    t0 = time.perf_counter()

    con = None
    if os.path.exists(state_db_path):
        con = duckdb.connect(state_db_path)
        con.execute("""
            CREATE TABLE IF NOT EXISTS compaction_log (
                output_file VARCHAR,
                input_file_count BIGINT,
                row_count BIGINT,
                compacted_at TIMESTAMP
            );
        """)

    for date_dir in date_dirs:
        candidates = [
            f for f in sorted(glob.glob(os.path.join(date_dir, "*.parquet")))
            if not os.path.basename(f).startswith("compacted-")
        ]
        if len(candidates) <= 1:
            continue

        print(f"Compacting {len(candidates)} files in {os.path.basename(date_dir)}...")

        tables = []
        merged_files = []
        for pf in candidates:
            try:
                tables.append(pq.read_table(pf))
                merged_files.append(pf)
            except Exception as e:
                skipped_unreadable.append(pf)
                print(f"Leaving unreadable file in place {pf}: {e}")

        if len(merged_files) <= 1:
            continue

        consolidated = pa.concat_tables(tables)
        expected_rows = sum(t.num_rows for t in tables)
        if consolidated.num_rows != expected_rows:
            raise RuntimeError(
                f"Row count mismatch in {date_dir}: merged {consolidated.num_rows}, inputs {expected_rows}"
            )
        rows_in += expected_rows

        final_compacted = _next_compacted_path(date_dir)
        temp_compacted = os.path.join(date_dir, f".{os.path.basename(final_compacted)}.tmp")

        pq.write_table(consolidated, temp_compacted, compression="snappy", row_group_size=100000)
        written_rows = pq.ParquetFile(temp_compacted).metadata.num_rows
        if written_rows != expected_rows:
            os.remove(temp_compacted)
            raise RuntimeError(f"Written file has {written_rows} rows, expected {expected_rows}")
        os.replace(temp_compacted, final_compacted)
        rows_out += written_rows

        for pf in merged_files:
            try:
                os.remove(pf)
            except OSError:
                pass

        if con:
            con.execute(
                "INSERT INTO compaction_log VALUES (?, ?, ?, current_timestamp)",
                [os.path.relpath(final_compacted, lake_dir), len(merged_files), written_rows],
            )

    if con:
        con.close()

    elapsed = time.perf_counter() - t0
    final_files = len(glob.glob(os.path.join(lake_dir, "**", "*.parquet"), recursive=True))

    result = {
        "initial_files": initial_files,
        "final_files": final_files,
        "files_reduced": initial_files - final_files,
        "rows_in_inputs": rows_in,
        "rows_in_outputs": rows_out,
        "unreadable_files_left": len(skipped_unreadable),
        "compaction_time_sec": round(elapsed, 3),
    }

    print(f"Compaction done: {initial_files} files to {final_files} in {result['compaction_time_sec']}s "
          f"({rows_out:,} rows)")
    return result


def main():
    parser = argparse.ArgumentParser(description="Compact small Parquet files in the lake.")
    parser.add_argument("--lake-dir", type=str, default=DEFAULT_LAKE_DIR, help="Lake directory to compact")
    parser.add_argument("--state-db", type=str, default=DEFAULT_STATE_DB, help="DuckDB state database path")
    args = parser.parse_args()

    compact_lake(lake_dir=args.lake_dir, state_db_path=args.state_db)


if __name__ == "__main__":
    main()
