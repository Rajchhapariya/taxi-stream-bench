"""Mode B restart: a renamed file that the manifest does not list is removed, listed files stay."""

import glob
import os

import pytest

from pipeline.sink import SinkConsumer
from tests.helpers import load_batch


def _make(tmp_path, monkeypatch, **kw):
    monkeypatch.setenv("LOCAL_BROKER_DIR", str(tmp_path / "broker"))
    lake = tmp_path / "lake"
    return SinkConsumer(mode="effectively-once", batch_size=50, batch_timeout=10.0, lake_dir=str(lake),
                        state_db_path=str(lake / "sink_state.duckdb"), force_local=True, **kw), str(lake)


def _files(lake):
    return sorted(os.path.basename(f) for f in glob.glob(os.path.join(lake, "**", "trips-p*.parquet"), recursive=True))


def _crash_after_rename(tmp_path, monkeypatch):
    sink, lake = _make(tmp_path, monkeypatch, crash_after_rename=True)
    load_batch(sink, 0, 0, 50)
    with pytest.raises(SystemExit):
        sink._flush_partition(0)
    sink.con.close()
    return lake


def test_unmanifested_file_is_removed_on_restart(tmp_path, monkeypatch):
    lake = _crash_after_rename(tmp_path, monkeypatch)
    assert _files(lake) == ["trips-p0-o0-o49.parquet"]
    restarted, _ = _make(tmp_path, monkeypatch)
    assert restarted.reconciled_files == 1
    assert _files(lake) == []
    restarted.con.close()


def test_without_reconcile_the_orphan_stays(tmp_path, monkeypatch):
    lake = _crash_after_rename(tmp_path, monkeypatch)
    restarted, _ = _make(tmp_path, monkeypatch, reconcile_orphans=False)
    assert _files(lake) == ["trips-p0-o0-o49.parquet"]
    restarted.con.close()


def test_manifested_file_is_kept(tmp_path, monkeypatch):
    sink, lake = _make(tmp_path, monkeypatch)
    load_batch(sink, 0, 0, 50)
    sink._flush_partition(0)
    sink.con.close()
    restarted, _ = _make(tmp_path, monkeypatch)
    assert restarted.reconciled_files == 0
    assert _files(lake) == ["trips-p0-o0-o49.parquet"]
    restarted.con.close()
