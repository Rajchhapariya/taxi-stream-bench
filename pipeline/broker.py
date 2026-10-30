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
    from confluent_kafka.admin import AdminClient, NewTopic
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False
    ConfluentProducer = None
    ConfluentConsumer = None
    KafkaError = None
    TopicPartition = None
    AdminClient = None
    NewTopic = None


def is_broker_accessible(host: str = "localhost", port: int = 9092, timeout: float = 0.5) -> bool:
    """Checks if a broker is listening on host:port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


class BrokerUnavailableError(RuntimeError):
    """Raised when a real Kafka-API broker was required but cannot be used."""


def _split_host_port(bootstrap_servers: str) -> Tuple[str, int]:
    first = bootstrap_servers.split(",")[0]
    host = first.split(":")[0]
    port = int(first.split(":")[1]) if ":" in first else 9092
    return host, port


def kafka_usable(bootstrap_servers: str) -> bool:
    """True when confluent-kafka is installed and something is listening on the broker address."""
    if not CONFLUENT_AVAILABLE:
        return False
    host, port = _split_host_port(bootstrap_servers)
    return is_broker_accessible(host, port)


def broker_label(bootstrap_servers: str = "localhost:9092", force_local: bool = False,
                 require_kafka: bool = False) -> str:
    """Names the broker a run will really use. Every benchmark result records this string."""
    if force_local:
        return "local-emulator"
    if kafka_usable(bootstrap_servers):
        return f"kafka-api@{bootstrap_servers}"
    if require_kafka:
        raise BrokerUnavailableError(
            f"No Kafka-API broker reachable at {bootstrap_servers} (or confluent-kafka is missing)."
        )
    return "local-emulator (fallback: no broker reachable)"


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

    def peek_end_offset(self, topic: str, partition: int) -> int:
        """Number of records currently in the partition log, read from disk (never cached).

        Safe to call from a process that is not the writer, which is how lag is measured.
        """
        _, idx_file = self._get_paths(topic, partition)
        if not os.path.exists(idx_file):
            return 0
        return os.path.getsize(idx_file) // 16

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

    def get_watermark_offsets(self, partition: Any, timeout: float = 2.0, cached: bool = False) -> Tuple[int, int]:
        p = partition.partition if hasattr(partition, "partition") else int(partition)
        topic = partition.topic if hasattr(partition, "topic") else self.subscribed_topics[0]
        high = self.engine.peek_end_offset(topic, p)
        return (0, high)

    def close(self):
        self.engine.close()


class KafkaConsumerAdapter:
    """Gives a confluent-kafka Consumer the small interface the sink uses.

    Two things differ from the emulator and are handled here:
    - seek() is not valid before partitions are assigned. When the sink asks to resume from
      externally stored offsets (Mode B), the adapter assigns all partitions with those offsets
      explicitly and does not join the consumer group.
    - commit() takes offsets as a keyword argument and is made synchronous, so Mode A really does
      write the file first and finish the commit second.
    """

    def __init__(self, consumer: Any, num_partitions: int = 6):
        self._c = consumer
        self._n = num_partitions
        self._topics: List[str] = []
        self._pending: Dict[int, int] = {}
        self._started = False

    def subscribe(self, topics: List[str]):
        self._topics = list(topics)

    def seek(self, tp: Any):
        self._pending[tp.partition] = tp.offset

    def _start(self):
        if self._started:
            return
        self._started = True
        if self._pending:
            topic = self._topics[0]
            self._c.assign([
                TopicPartition(topic, p, self._pending.get(p, confluent_kafka.OFFSET_BEGINNING))
                for p in range(self._n)
            ])
        else:
            self._c.subscribe(self._topics)

    def poll(self, timeout: float = 0.05):
        self._start()
        return self._c.poll(timeout)

    def commit(self, offsets: Optional[List[Any]] = None):
        self._start()
        if offsets is None:
            self._c.commit(asynchronous=False)
        else:
            self._c.commit(offsets=offsets, asynchronous=False)

    def get_watermark_offsets(self, partition: Any, timeout: float = 2.0, cached: bool = False) -> Tuple[int, int]:
        return self._c.get_watermark_offsets(partition, timeout=timeout, cached=cached)

    def close(self):
        self._c.close()


def create_kafka_topic(topic: str, bootstrap_servers: str = "localhost:9092",
                       num_partitions: int = 6, replication_factor: int = 1,
                       timeout: float = 10.0) -> None:
    """Creates a topic with the specified partition count. Fails loudly if unreachable."""
    if not CONFLUENT_AVAILABLE or AdminClient is None:
        raise BrokerUnavailableError("confluent-kafka AdminClient is not available.")
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
    futures = admin.create_topics([new_topic], request_timeout=timeout)
    for t, future in futures.items():
        try:
            future.result(timeout=timeout)
        except Exception as e:
            err_str = str(e).lower()
            if "already exists" not in err_str and "topic_already_exists" not in err_str:
                raise


def delete_kafka_topic(topic: str, bootstrap_servers: str = "localhost:9092",
                       timeout: float = 10.0) -> None:
    """Deletes a Kafka topic if it exists."""
    if not CONFLUENT_AVAILABLE or AdminClient is None:
        return
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.delete_topics([topic], request_timeout=timeout)
    for t, future in futures.items():
        try:
            future.result(timeout=timeout)
        except Exception:
            pass


def get_redpanda_version(admin_url: str = "http://localhost:9644") -> Optional[str]:
    """Queries Redpanda Admin HTTP endpoint if available."""
    try:
        import urllib.request
        with urllib.request.urlopen(f"{admin_url}/v1/brokers", timeout=1.5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, list) and data and "version" in data[0]:
                raw = data[0]["version"]
                return raw.split()[0] if raw else None
    except Exception:
        pass
    try:
        res = subprocess.run(["docker", "exec", "redpanda-broker", "rpk", "version"],
                             capture_output=True, text=True, timeout=2.0)
        if res.returncode == 0:
            for line in res.stdout.splitlines():
                if line.startswith("Version:"):
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return None


def get_topic_watermark_sum(topic: str, bootstrap_servers: str = "localhost:9092",
                            num_partitions: int = 6, consumer: Optional[Any] = None) -> int:
    """Queries broker for the sum of high watermark offsets across all partitions."""
    close_when_done = False
    if consumer is None:
        if not CONFLUENT_AVAILABLE:
            return 0
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"wm-query-{os.getpid()}-{int(time.time()*1000)}",
            "enable.auto.commit": False,
        }
        consumer = ConfluentConsumer(conf)
        close_when_done = True
    try:
        total = 0
        for p in range(num_partitions):
            tp = TopicPartition(topic, p)
            res = consumer.get_watermark_offsets(tp, timeout=2.0)
            if res is not None:
                total += res[1]
        return total
    finally:
        if close_when_done:
            try:
                consumer.close()
            except Exception:
                pass


def get_default_local_state_dir() -> str:
    env_dir = os.environ.get("LOCAL_BROKER_DIR")
    if env_dir:
        return env_dir
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".broker_state",
    )


def create_producer(bootstrap_servers: str = "localhost:9092", force_local: bool = False,
                    require_kafka: bool = False) -> Any:
    """Kafka-API producer when a broker is reachable, otherwise the local emulator.

    With require_kafka=True the emulator is never used: an unreachable broker raises
    BrokerUnavailableError instead of silently changing what is being measured.
    """
    broker_label(bootstrap_servers, force_local, require_kafka)  # raises when required and absent
    if not force_local and kafka_usable(bootstrap_servers):
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
    require_kafka: bool = False,
    num_partitions: int = 6,
) -> Any:
    broker_label(bootstrap_servers, force_local, require_kafka)
    if not force_local and kafka_usable(bootstrap_servers):
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "fetch.min.bytes": 1024 * 32,
            "fetch.wait.max.ms": 100,
            # A killed consumer stays a group member until this timeout; keep it short so a
            # restarted Mode A sink is not blocked for the 45 s default.
            "session.timeout.ms": 6000,
        }
        return KafkaConsumerAdapter(ConfluentConsumer(conf), num_partitions=num_partitions)

    engine = LocalBrokerEngine(state_dir=get_default_local_state_dir())
    return LocalConsumer(engine, group_id=group_id)


if TopicPartition is None:
    TopicPartition = EmulatedTopicPartition
