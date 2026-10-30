"""Helpers shared by the benchmark scripts.

Every result file written through write_result() carries the broker that produced it and the
environment it ran in, so a number can never be read without knowing what it measured.
"""

from pipeline.broker import (LocalBrokerEngine, broker_label, create_kafka_topic,
                             delete_kafka_topic, get_default_local_state_dir,
                             get_redpanda_version, get_topic_watermark_sum, kafka_usable)
import duckdb
import glob
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "yellow_tripdata_2024-01.parquet")
RESULTS_DIR = os.path.join(BASE_DIR, "results")


def _pkg_version(name: str) -> str:
    try:
        from importlib.metadata import version
        return version(name)
    except Exception:
        return "not installed"


def _cpu_model() -> str:
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def _ram_mb() -> Optional[int]:
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    return None


def _disk_kind() -> str:
    try:
        flags = []
        for path in glob.glob("/sys/block/*/queue/rotational"):
            with open(path, encoding="utf-8") as f:
                flags.append(
                    f"{path.split('/')[3]}={'HDD' if f.read().strip() == '1' else 'non-rotational'}")
        return ", ".join(flags) or "unknown"
    except OSError:
        return "unknown"


def environment_info() -> Dict[str, Any]:
    return {
        "os": platform.platform(),
        "python": platform.python_version(),
        "cpu": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "ram_mb": _ram_mb(),
        "disk": _disk_kind(),
        "docker": "not available in this environment",
        "redpanda": "not run",
        "libraries": {
            "duckdb": _pkg_version("duckdb"),
            "pyarrow": _pkg_version("pyarrow"),
            "confluent-kafka": _pkg_version("confluent-kafka"),
            "numpy": _pkg_version("numpy"),
        },
    }


def result_filename(base_name: str, broker: str = "local") -> str:
    """Returns the output JSON file name distinguished by broker (e.g. *_redpanda.json)."""
    if broker == "kafka":
        return base_name.replace(".json", "_redpanda.json")
    return base_name


def write_result(name: str, payload: Dict[str, Any], force_local: bool = True,
                 bootstrap_servers: str = "localhost:9092", require_kafka: bool = False,
                 broker: str = "local") -> Dict[str, Any]:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    is_kafka = (broker == "kafka") or (
        not force_local and kafka_usable(bootstrap_servers))
    real_force_local = not is_kafka
    doc = {
        "broker": broker_label(bootstrap_servers, force_local=real_force_local, require_kafka=require_kafka),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "environment": environment_info(),
    }
    redpanda_ver = get_redpanda_version()
    if redpanda_ver:
        doc["redpanda_version"] = redpanda_ver
    doc.update(payload)
    out_path = os.path.join(RESULTS_DIR, name)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    return doc


def hard_kill_process(proc: subprocess.Popen) -> None:
    """Forcefully terminates a subprocess without graceful shutdown."""
    if proc.poll() is not None:
        return
    if sys.platform == "win32":
        try:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        except Exception:
            proc.kill()
    else:
        proc.kill()
    try:
        proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        pass


def is_valid_parquet(path: str) -> bool:
    """Checks if a Parquet file has valid size and magic footer bytes."""
    try:
        if os.path.getsize(path) < 12:
            return False
        with open(path, "rb") as f:
            f.seek(-4, os.SEEK_END)
            return f.read(4) == b"PAR1"
    except Exception:
        return False


def lake_stats(lake_dir: str) -> Dict[str, Any]:
    all_files = glob.glob(os.path.join(
        lake_dir, "**", "*.parquet"), recursive=True)
    if not all_files:
        return {"files": 0, "rows": 0, "unique_row_ids": 0, "bytes": 0}
    # Filter out files truncated mid-write by hard-kills
    files = [f for f in all_files if is_valid_parquet(f)]
    if not files:
        return {"files": 0, "rows": 0, "unique_row_ids": 0, "bytes": 0}
    norm_files = [f.replace("\\", "/") for f in files]
    con = duckdb.connect()
    rows, unique = con.execute(
        "SELECT count(*), count(DISTINCT row_id) FROM read_parquet(?)", [
            norm_files]
    ).fetchone()
    con.close()
    return {
        "files": len(files),
        "rows": int(rows),
        "unique_row_ids": int(unique),
        "bytes": sum(os.path.getsize(f) for f in files),
    }


def lake_row_ids(lake_dir: str) -> Set[int]:
    all_files = glob.glob(os.path.join(
        lake_dir, "**", "*.parquet"), recursive=True)
    if not all_files:
        return set()
    files = [f for f in all_files if is_valid_parquet(f)]
    if not files:
        return set()
    norm_files = [f.replace("\\", "/") for f in files]
    con = duckdb.connect()
    ids = {r[0] for r in con.execute(
        "SELECT DISTINCT row_id FROM read_parquet(?)", [norm_files]).fetchall()}
    con.close()
    return ids


def produced_row_ids(broker_dir: str, topic: str, partitions: int = 6) -> Set[int]:
    """Reads every row_id the producer put on the topic, straight from the emulator log."""
    engine = LocalBrokerEngine(state_dir=broker_dir, num_partitions=partitions)
    ids: Set[int] = set()
    for p in range(partitions):
        for off in range(engine.peek_end_offset(topic, p)):
            msg = engine.read_record_at_offset(topic, p, off)
            if msg is not None:
                ids.add(json.loads(msg.value().decode("utf-8"))["row_id"])
    engine.close()
    return ids


def topic_end_total(broker_dir: str, topic: str, partitions: int = 6) -> int:
    engine = LocalBrokerEngine(state_dir=broker_dir, num_partitions=partitions)
    total = sum(engine.peek_end_offset(topic, p) for p in range(partitions))
    engine.close()
    return total


def topic_end_total_agnostic(topic: str, broker: str = "local", broker_dir: Optional[str] = None,
                             bootstrap_servers: str = "localhost:9092", partitions: int = 6,
                             consumer: Optional[Any] = None) -> int:
    """Returns the total number of records across all partitions for either broker."""
    if broker == "kafka":
        return get_topic_watermark_sum(topic, bootstrap_servers=bootstrap_servers,
                                       num_partitions=partitions, consumer=consumer)
    else:
        bdir = broker_dir or get_default_local_state_dir()
        return topic_end_total(bdir, topic, partitions=partitions)


def start_producer_process(topic: str, broker_dir: Optional[str] = None, rate: Optional[float] = None,
                           max_records: int = 0, stats_out: str = "", broker: str = "local",
                           bootstrap_servers: str = "localhost:9092") -> subprocess.Popen:
    cmd = [
        sys.executable, "-u", "-m", "pipeline.producer",
        "--topic", topic, "--max-records", str(max_records),
        "--stats-out", stats_out,
    ]
    if broker == "kafka":
        cmd += ["--bootstrap-servers", bootstrap_servers, "--require-kafka"]
    else:
        cmd += ["--force-local"]
    if rate:
        cmd += ["--rate", str(rate)]
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR
    if broker_dir and broker == "local":
        env["LOCAL_BROKER_DIR"] = broker_dir
    return subprocess.Popen(cmd, cwd=BASE_DIR, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    k = (len(xs) - 1) * q / 100.0
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


# ---- resumable runs -------------------------------------------------------------------------
# A single shell call in some environments is capped at a few minutes, so every benchmark saves
# each finished trial to results/_partial_<name>.json and skips trials that are already there.

def _partial_path(name: str) -> str:
    return os.path.join(RESULTS_DIR, f"_partial_{name}.json")


def load_partial(name: str) -> Dict[str, Any]:
    try:
        with open(_partial_path(name), encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_partial(name: str, data: Dict[str, Any]) -> None:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(_partial_path(name), "w", encoding="utf-8") as f:
        json.dump(data, f)


def clear_partial(name: str) -> None:
    try:
        os.remove(_partial_path(name))
    except OSError:
        pass


class TrialBudget:
    """Stops a benchmark after max_new trials so the next call can resume it."""

    def __init__(self, max_new: Optional[int]):
        self.left = max_new

    def take(self) -> bool:
        if self.left is None:
            return True
        if self.left <= 0:
            return False
        self.left -= 1
        return True
