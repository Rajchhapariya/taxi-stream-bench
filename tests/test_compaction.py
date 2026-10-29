"""Compaction must keep every row, never reuse an output name and never delete a file it could not read."""

import glob
import os

import duckdb
import pyarrow.parquet as pq

from pipeline.compaction import compact_lake
from tests.helpers import sample_records
from pipeline.sink import SinkConsumer


def _write_small_files(lake, start, n_files, rows):
    d = os.path.join(lake, "date=2024-01-15")
    os.makedirs(d, exist_ok=True)
    for i in range(n_files):
        recs = sample_records(start + i * rows, rows)
        sink = SinkConsumer.__new__(SinkConsumer)
        table = SinkConsumer._records_to_arrow_table(sink, recs, 1)
        pq.write_table(table, os.path.join(d, f"trips-p0-o{start + i * rows}-o{start + (i + 1) * rows - 1}.parquet"))


def _count(lake):
    return duckdb.sql(f"select count(*), count(distinct row_id) from read_parquet('{lake}/**/*.parquet')").fetchone()


def test_two_compactions_keep_all_rows(tmp_path):
    lake = str(tmp_path / "lake")
    _write_small_files(lake, 0, 5, 20)
    r1 = compact_lake(lake, str(tmp_path / "none.duckdb"))
    assert (r1["rows_in_inputs"], r1["rows_in_outputs"]) == (100, 100)
    _write_small_files(lake, 100, 4, 20)
    r2 = compact_lake(lake, str(tmp_path / "none.duckdb"))
    assert r2["rows_in_outputs"] == 80
    assert _count(lake) == (180, 180)
    assert len(glob.glob(os.path.join(lake, "**", "compacted-*.parquet"), recursive=True)) == 2


def test_unreadable_file_is_left_in_place(tmp_path):
    lake = str(tmp_path / "lake")
    _write_small_files(lake, 0, 4, 20)
    bad = os.path.join(lake, "date=2024-01-15", "trips-p9-o0-o9.parquet")
    with open(bad, "wb") as f:
        f.write(b"not a parquet file")
    result = compact_lake(lake, str(tmp_path / "none.duckdb"))
    assert result["unreadable_files_left"] == 1
    assert os.path.exists(bad)
