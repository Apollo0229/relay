"""Runs the worker against a real Kafka broker. Skipped unless WORKER_TEST_KAFKA_BROKERS is set."""

import json
import os
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from conftest import counts

from worker.consumer import Worker
from worker.handlers import count_event
from worker.processing import Processor
from worker.settings import Settings

BROKERS = os.environ.get("WORKER_TEST_KAFKA_BROKERS")
pytestmark = pytest.mark.skipif(not BROKERS, reason="WORKER_TEST_KAFKA_BROKERS is not set")


@pytest.fixture
def settings():
    run = uuid.uuid4().hex[:8]
    s = Settings(kafka_brokers=BROKERS, topic=f"test-{run}", group_id=f"test-{run}", retry_delays=[0.3, 0.3])
    topics = [s.topic, *s.retry_topics, s.dead_letter_topic]
    admin = AdminClient({"bootstrap.servers": BROKERS})
    for future in admin.create_topics([NewTopic(t, num_partitions=3) for t in topics]).values():
        future.result()
    return s


def envelope(source: str, type_: str, **data) -> dict:
    return {"id": str(uuid.uuid4()), "source": source, "type": type_, "data": data, "received_at": "now"}


def produce(settings: Settings, messages: list[tuple[str, bytes]]) -> None:
    producer = Producer({"bootstrap.servers": BROKERS})
    for key, value in messages:
        producer.produce(settings.topic, key=key, value=value)
    assert producer.flush(10) == 0


def read_all(topic: str, expected: int, timeout: float = 20) -> list:
    consumer = Consumer(
        {"bootstrap.servers": BROKERS, "group.id": f"reader-{uuid.uuid4()}", "auto.offset.reset": "earliest"}
    )
    consumer.subscribe([topic])
    out, deadline = [], time.time() + timeout
    while len(out) < expected and time.time() < deadline:
        msg = consumer.poll(0.5)
        if msg is not None and not msg.error():
            out.append(msg)
    consumer.close()
    return out


def wait_for(condition, timeout: float = 20) -> None:
    deadline = time.time() + timeout
    while not condition():
        assert time.time() < deadline, "condition not met in time"
        time.sleep(0.1)


def test_events_are_counted_once_retried_and_dead_lettered(pool, settings):
    ok = envelope("stripe", "payment.succeeded")
    flaky = envelope("github", "push", simulate="flaky")
    failing = envelope("shopify", "order.created", simulate="error")
    invalid = envelope("shopify", "order.paid", simulate="invalid")
    messages = [(e["source"], json.dumps(e).encode()) for e in (ok, flaky, failing, invalid)]
    messages += [("stripe", json.dumps(ok).encode()), ("junk", b"not json")]  # a redelivery and garbage

    processor = Processor(
        pool, settings.consumer, count_event, settings.retry_topics, settings.dead_letter_topic
    )
    worker = Worker(settings, processor)
    worker.start()
    try:
        produce(settings, messages)
        dead = read_all(settings.dead_letter_topic, expected=3)
        wait_for(lambda: len(counts(pool)) == 2)
    finally:
        worker.stop()

    assert not worker.failed.is_set()
    assert counts(pool) == {("stripe", "payment.succeeded"): 1, ("github", "push"): 1}

    by_value = {m.value(): dict((k, v.decode()) for k, v in m.headers()) for m in dead}
    assert by_value[json.dumps(failing).encode()]["attempt"] == "3"
    assert by_value[json.dumps(failing).encode()]["error"] == "RuntimeError: simulated downstream outage"
    assert by_value[json.dumps(invalid).encode()]["attempt"] == "1"
    assert by_value[b"not json"]["error"].startswith("PermanentError: malformed event")
    assert len(dead) == 3
