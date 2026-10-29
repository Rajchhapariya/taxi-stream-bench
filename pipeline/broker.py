"""Streaming broker client and local fallback emulator.

Wraps confluent-kafka for Redpanda/Kafka clusters, and provides a disk-backed
multi-partition log emulator so tests and local benchmarks can run without Docker.
"""

import json
import os
import socket
import struct
import time
import zlib
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import confluent_kafka
    from confluent_kafka import Consumer as ConfluentConsumer
    from confluent_kafka import Producer as ConfluentProducer
    from confluent_kafka import KafkaError, TopicPartition
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False
    ConfluentProducer = None
    ConfluentConsumer = None
    KafkaError = None
    TopicPartition = None


def is_broker_accessible(host: str = "localhost", port: int = 9092, timeout: float = 0.5) -> bool:
    """Checks if a broker is listening on host:port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


class EmulatedMessage:
    """Matches the confluent_kafka.Message interface."""

    def __init__(self, topic: str, partition: int, offset: int, key: bytes, value: bytes, timestamp_ms: int):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._timestamp = (1, timestamp_ms)

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes:
        return self._key

    def value(self) -> bytes:
        return self._value

    def timestamp(self) -> Tuple[int, int]:
        return self._timestamp

    def error(self):
        return None


class EmulatedTopicPartition:
    """Matches the confluent_kafka.TopicPartition interface."""

    def __init__(self, topic: str, partition: int, offset: int = -1001):
        self.topic = topic
        self.partition = partition
        self.offset = offset

    def __repr__(self):
        return f"TopicPartition(topic='{self.topic}', partition={self.partition}, offset={self.offset})"


class LocalBrokerEngine:
    """Disk-backed partition log for testing without Docker.

    Writes records to append-only logs with 16-byte index entries
    (8-byte offset, 8-byte file position) for fast seeks.
    """

    def __init__(self, state_dir: str, num_partitions: int = 6):
        self.state_dir = state_dir
        self.num_partitions = num_partitions
        os.makedirs(self.state_dir, exist_ok=True)
        self.offsets_file = os.path.join(self.state_dir, "consumer_offsets.json")
        self._next_offsets: Dict[Tuple[str, int], int] = {}
        self._log_writers: Dict[Tuple[str, int], Any] = {}
        self._idx_writers: Dict[Tuple[str, int], Any] = {}

    def _get_paths(self, topic: str, partition: int) -> Tuple[str, str]:
        t_dir = os.path.join(self.state_dir, topic)
        os.makedirs(t_dir, exist_ok=True)
        log_file = os.path.join(t_dir, f"partition_{partition}.log")
        idx_file = os.path.join(t_dir, f"partition_{partition}.idx")
        return log_file, idx_file

    def _get_next_offset(self, topic: str, partition: int) -> int:
        key = (topic, partition)
        if key in self._next_offsets:
            return self._next_offsets[key]

        _, idx_file = self._get_paths(topic, partition)
        if not os.path.exists(idx_file):
            self._next_offsets[key] = 0
            return 0

        sz = os.path.getsize(idx_file)
        if sz >= 16:
            with open(idx_file, "rb") as f:
                f.seek(sz - 16)
                last_off, _ = struct.unpack(">QQ", f.read(16))
                next_off = last_off + 1
        else:
            next_off = 0

        self._next_offsets[key] = next_off
        return next_off

    def write_record(self, topic: str, partition: int, key: bytes, value: bytes) -> int:
        log_path, idx_path = self._get_paths(topic, partition)
        key_pair = (topic, partition)

        if key_pair not in self._log_writers:
            self._log_writers[key_pair] = open(log_path, "ab")
            self._idx_writers[key_pair] = open(idx_path, "ab")

        log_f = self._log_writers[key_pair]
        idx_f = self._idx_writers[key_pair]

        offset = self._get_next_offset(topic, partition)
        pos = log_f.tell()
        ts_ms = int(time.time() * 1000)

        # Record header: magic (2B), offset (8B), timestamp (8B), key_len (4B), val_len (4B)
        header = struct.pack(">HQQII", 0xABCD, offset, ts_ms, len(key), len(value))
        log_f.write(header)
        log_f.write(key)
        log_f.write(value)
        log_f.flush()

        idx_f.write(struct.pack(">QQ", offset, pos))
        idx_f.flush()

        self._next_offsets[key_pair] = offset + 1
        return offset

    def read_record_at_offset(self, topic: str, partition: int, target_offset: int) -> Optional[EmulatedMessage]:
        log_path, idx_path = self._get_paths(topic, partition)
        if not os.path.exists(idx_path) or not os.path.exists(log_path):
            return None

        idx_size = os.path.getsize(idx_path)
        entry_idx = target_offset
        byte_pos = entry_idx * 16

        if byte_pos + 16 > idx_size:
            return None

        with open(idx_path, "rb") as idx_f:
            idx_f.seek(byte_pos)
            off, file_pos = struct.unpack(">QQ", idx_f.read(16))
            if off != target_offset:
                return None

        with open(log_path, "rb") as log_f:
            log_f.seek(file_pos)
            hdr_bytes = log_f.read(26)
            if len(hdr_bytes) < 26:
                return None
            magic, offset, ts_ms, klen, vlen = struct.unpack(">HQQII", hdr_bytes)
            if magic != 0xABCD:
                return None
            k = log_f.read(klen)
            v = log_f.read(vlen)
            return EmulatedMessage(topic, partition, offset, k, v, ts_ms)

    def load_committed_offsets(self, group_id: str) -> Dict[str, Dict[int, int]]:
        if os.path.exists(self.offsets_file):
            try:
                with open(self.offsets_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get(group_id, {})
            except (json.JSONDecodeError, OSError):
                return {}
        return {}

    def save_committed_offset(self, group_id: str, topic: str, partition: int, offset: int):
        data: Dict[str, Any] = {}
        if os.path.exists(self.offsets_file):
            try:
                with open(self.offsets_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                data = {}

        if group_id not in data:
            data[group_id] = {}
        if topic not in data[group_id]:
            data[group_id][topic] = {}

        data[group_id][topic][str(partition)] = offset

        tmp_path = f"{self.offsets_file}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, self.offsets_file)

    def close(self):
        for f in self._log_writers.values():
            try:
                f.close()
            except Exception:
                pass
        for f in self._idx_writers.values():
            try:
                f.close()
            except Exception:
                pass
        self._log_writers.clear()
        self._idx_writers.clear()


class LocalProducer:
    def __init__(self, engine: LocalBrokerEngine):
        self.engine = engine

    def produce(self, topic: str, key: bytes, value: bytes, partition: Optional[int] = None):
        if partition is None:
            # Deterministic hash partitioning on message key
            h = zlib.crc32(key) if key else 0
            partition = h % self.engine.num_partitions
        self.engine.write_record(topic, partition, key, value)

    def poll(self, timeout: float = 0):
        pass

    def flush(self):
        pass


class LocalConsumer:
    def __init__(self, engine: LocalBrokerEngine, group_id: str, num_partitions: int = 6):
        self.engine = engine
        self.group_id = group_id
        self.num_partitions = num_partitions
        self.subscribed_topics: List[str] = []
        self.current_offsets: Dict[int, int] = {p: 0 for p in range(num_partitions)}
        self._active_partition_cursor = 0

    def subscribe(self, topics: List[str]):
        self.subscribed_topics = topics
        if topics:
            t = topics[0]
            saved = self.engine.load_committed_offsets(self.group_id).get(t, {})
            for p in range(self.num_partitions):
                str_p = str(p)
                if str_p in saved:
                    self.current_offsets[p] = saved[str_p]
                else:
                    self.current_offsets[p] = 0

    def poll(self, timeout: float = 0.05) -> Optional[EmulatedMessage]:
        if not self.subscribed_topics:
            return None
        topic = self.subscribed_topics[0]

        start_cursor = self._active_partition_cursor
        for i in range(self.num_partitions):
            p = (start_cursor + i) % self.num_partitions
            cur_off = self.current_offsets[p]
            msg = self.engine.read_record_at_offset(topic, p, cur_off)
            if msg is not None:
                self.current_offsets[p] = cur_off + 1
                self._active_partition_cursor = (p + 1) % self.num_partitions
                return msg

        self._active_partition_cursor = (start_cursor + 1) % self.num_partitions
        if timeout > 0:
            time.sleep(min(timeout, 0.02))
        return None

    def commit(self, offsets: Optional[List[Any]] = None):
        if not self.subscribed_topics:
            return
        topic = self.subscribed_topics[0]

        if offsets is None:
            for p, off in self.current_offsets.items():
                self.engine.save_committed_offset(self.group_id, topic, p, off)
        else:
            for tp in offsets:
                self.engine.save_committed_offset(self.group_id, tp.topic, tp.partition, tp.offset)

    def seek(self, partition: Any):
        p = partition.partition
        off = partition.offset
        self.current_offsets[p] = off

    def close(self):
        self.engine.close()


def get_default_local_state_dir() -> str:
    env_dir = os.environ.get("LOCAL_BROKER_DIR")
    if env_dir:
        return env_dir
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".broker_state",
    )


def create_producer(bootstrap_servers: str = "localhost:9092", force_local: bool = False) -> Any:
    if not force_local and CONFLUENT_AVAILABLE:
        host = bootstrap_servers.split(":")[0]
        port = int(bootstrap_servers.split(":")[1]) if ":" in bootstrap_servers else 9092
        if is_broker_accessible(host, port):
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "taxi-stream-producer",
                "queue.buffering.max.messages": 200000,
                "queue.buffering.max.kbytes": 262144,
                "batch.num.messages": 10000,
                "linger.ms": 5,
                "compression.type": "snappy",
                "acks": 1,
            }
            return ConfluentProducer(conf)

    engine = LocalBrokerEngine(state_dir=get_default_local_state_dir())
    return LocalProducer(engine)


def create_consumer(
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "taxi-sink-group",
    force_local: bool = False,
) -> Any:
    if not force_local and CONFLUENT_AVAILABLE:
        host = bootstrap_servers.split(":")[0]
        port = int(bootstrap_servers.split(":")[1]) if ":" in bootstrap_servers else 9092
        if is_broker_accessible(host, port):
            conf = {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "fetch.min.bytes": 1024 * 32,
                "fetch.wait.max.ms": 100,
            }
            return ConfluentConsumer(conf)

    engine = LocalBrokerEngine(state_dir=get_default_local_state_dir())
    return LocalConsumer(engine, group_id=group_id)


if TopicPartition is None:
    TopicPartition = EmulatedTopicPartition
