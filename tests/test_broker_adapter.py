"""The Kafka-API code path cannot be run without a broker, so these tests check how the adapter
calls confluent-kafka using a stand-in consumer: commit must pass offsets by keyword and be
synchronous, and resuming from stored offsets must assign partitions instead of seeking before
assignment (which confluent-kafka rejects)."""

import pytest

import pipeline.broker as broker
from pipeline.broker import (BrokerUnavailableError, EmulatedTopicPartition, KafkaConsumerAdapter,
                             broker_label, create_consumer, create_producer)


class FakeConfluentConsumer:
    def __init__(self):
        self.calls = []

    def subscribe(self, topics):
        self.calls.append(("subscribe", topics))

    def assign(self, tps):
        self.calls.append(("assign", [(t.topic, t.partition, t.offset) for t in tps]))

    def poll(self, timeout):
        self.calls.append(("poll", timeout))
        return None

    def commit(self, message=None, offsets=None, asynchronous=True):
        self.calls.append(("commit", message, offsets, asynchronous))

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        self.calls.append(("get_watermark_offsets", tp.topic, tp.partition))
        return (0, 1234)

    def assignment(self):
        self.calls.append(("assignment",))
        return [EmulatedTopicPartition("trips", 0)]

    def close(self):
        self.calls.append(("close",))


@pytest.fixture(autouse=True)
def tp_class(monkeypatch):
    # confluent_kafka's TopicPartition may not be importable in every environment
    monkeypatch.setattr(broker, "TopicPartition", EmulatedTopicPartition)


def test_commit_uses_offsets_keyword_and_is_synchronous():
    fake = FakeConfluentConsumer()
    adapter = KafkaConsumerAdapter(fake, num_partitions=6)
    adapter.subscribe(["trips"])
    tp = EmulatedTopicPartition("trips", 2, 1000)
    adapter.commit([tp])
    commit_call = [c for c in fake.calls if c[0] == "commit"][0]
    assert commit_call[1] is None          # not passed as the positional message argument
    assert commit_call[2] == [tp]
    assert commit_call[3] is False          # synchronous: the file write really precedes the commit


def test_group_subscription_when_no_resume_offsets():
    fake = FakeConfluentConsumer()
    adapter = KafkaConsumerAdapter(fake)
    adapter.subscribe(["trips"])
    adapter.poll(0.05)
    assert fake.calls[0] == ("subscribe", ["trips"])
    assert not any(c[0] == "assign" for c in fake.calls)


def test_resume_offsets_are_assigned_explicitly_before_first_poll():
    fake = FakeConfluentConsumer()
    adapter = KafkaConsumerAdapter(fake, num_partitions=3)
    adapter.subscribe(["trips"])
    adapter.seek(EmulatedTopicPartition("trips", 1, 5000))
    adapter.poll(0.05)
    assign = [c for c in fake.calls if c[0] == "assign"][0]
    assert assign[1][1] == ("trips", 1, 5000)
    assert len(assign[1]) == 3              # every partition is assigned, unseen ones from the beginning
    assert not any(c[0] == "subscribe" for c in fake.calls)


def test_label_names_the_emulator_when_forced():
    assert broker_label(force_local=True) == "local-emulator"


def test_require_kafka_refuses_to_fall_back(monkeypatch):
    monkeypatch.setattr(broker, "kafka_usable", lambda servers: False)
    with pytest.raises(BrokerUnavailableError):
        create_consumer(require_kafka=True)
    with pytest.raises(BrokerUnavailableError):
        create_producer(require_kafka=True)
    assert "fallback" in broker_label("localhost:9092")


def test_consumer_adapter_delegates_get_watermark_offsets():
    fake = FakeConfluentConsumer()
    adapter = KafkaConsumerAdapter(fake, num_partitions=6)
    tp = EmulatedTopicPartition("trips", 3)
    offsets = adapter.get_watermark_offsets(tp)
    assert offsets == (0, 1234)
    assert ("get_watermark_offsets", "trips", 3) in fake.calls


def test_local_consumer_get_watermark_offsets(tmp_path):
    engine = broker.LocalBrokerEngine(state_dir=str(tmp_path), num_partitions=6)
    consumer = broker.LocalConsumer(engine, group_id="test-grp", num_partitions=6)
    consumer.subscribe(["trips"])
    tp = EmulatedTopicPartition("trips", 0)
    offsets = consumer.get_watermark_offsets(tp)
    assert offsets == (0, 0)
    consumer.close()
