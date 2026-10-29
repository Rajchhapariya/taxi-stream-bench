"""A simulated crash must behave like a killed process: nothing is flushed afterwards."""

import glob
import os

import pytest

from pipeline.sink import SinkConsumer
from tests.helpers import load_batch


def _sink(tmp_path, monkeypatch, mode, **kw):
    monkeypatch.setenv("LOCAL_BROKER_DIR", str(tmp_path / "broker"))
    lake = tmp_path / "lake"
    return SinkConsumer(mode=mode, batch_size=50, batch_timeout=10.0, lake_dir=str(lake),
                        state_db_path=str(lake / "sink_state.duckdb"), force_local=True, **kw), str(lake)


def test_mode_a_crash_after_write_does_not_flush_again_or_commit(tmp_path, monkeypatch):
    sink, lake = _sink(tmp_path, monkeypatch, "at-least-once", crash_after_rename=True)
    sink.consumer.subscribe(["trips"])
    load_batch(sink, 0, 0, 50)
    with pytest.raises(SystemExit) as exc:
        sink._flush_partition(0)
    assert exc.value.code == 42
    assert sink._crashed
    assert len(glob.glob(os.path.join(lake, "**", "*.parquet"), recursive=True)) == 1
    # the offset was never committed, so a restart replays the batch
    assert sink.consumer.engine.load_committed_offsets("taxi-sink-group") == {}


def test_run_does_not_flush_buffers_after_a_crash(tmp_path, monkeypatch):
    sink, lake = _sink(tmp_path, monkeypatch, "at-least-once", crash_after_rename=True)
    sink.consumer.subscribe(["trips"])
    load_batch(sink, 0, 0, 50)
    sink.running = True
    sink.consumer.poll = lambda timeout=0.05: None
    # make run() hit the crash through its timer path
    sink.last_flush_times[0] = 0.0
    sink.batch_timeout = 0.0
    with pytest.raises(SystemExit):
        sink.run(idle_exit_polls=None)
    # the finally block must not have written the same batch a second time
    assert len(glob.glob(os.path.join(lake, "**", "*.parquet"), recursive=True)) == 1


def test_flush_records_reason_and_latencies(tmp_path, monkeypatch):
    sink, _ = _sink(tmp_path, monkeypatch, "effectively-once")
    load_batch(sink, 2, 0, 50)
    sink._flush_partition(2, "timeout")
    assert [(p, r, len(l)) for p, r, l, _t in sink.batch_latencies] == [(2, "timeout", 50)]
