"""Kafka plumbing: one consumer thread per tier, the main topic plus each delay topic.

A message forwarded to a delay topic carries a not-before time. Every message in
a delay topic waits the same time, so they come due in the order they arrive,
and a tier's thread only ever waits for the message at its head. Offsets are
committed after a message is handled or forwarded, so a crash means a
redelivery, which the processed_events check absorbs. That same check is why
offsets can be committed in the background once a second instead of per message:
a commit lost in a crash costs skipped duplicates, not a second effect.
"""

import logging
import threading
import time
from dataclasses import dataclass

import psycopg
from confluent_kafka import Consumer, KafkaException, Message, Producer, TopicPartition

from .handlers import Event, PermanentError
from .processing import Processor
from .settings import Settings

log = logging.getLogger(__name__)

ATTEMPT, NOT_BEFORE, ERROR = "attempt", "not-before", "error"


@dataclass(frozen=True)
class Tier:
    topic: str
    delay: float  # seconds a message forwarded here waits before it runs


class Forwarder:
    """Re-publishes a message to a delay or dead-letter topic and waits for the ack."""

    def __init__(self, brokers: str) -> None:
        self.producer = Producer({"bootstrap.servers": brokers, "enable.idempotence": True})

    def send(self, topic: str, msg: Message, attempt: int, error: str, delay: float = 0) -> None:
        failure: list[KafkaException] = []

        def on_delivery(err, _msg):
            if err is not None:
                failure.append(KafkaException(err))

        headers = {ATTEMPT: str(attempt), ERROR: error, NOT_BEFORE: str(int((time.time() + delay) * 1000))}
        self.producer.produce(
            topic, key=msg.key(), value=msg.value(), headers=headers, on_delivery=on_delivery
        )
        if self.producer.flush(30) > 0:
            raise KafkaException(f"timed out forwarding a message to {topic}")
        if failure:
            raise failure[0]


class Worker:
    """Runs one consumer thread per tier until stop() is called."""

    def __init__(self, settings: Settings, processor: Processor) -> None:
        self.settings = settings
        self.processor = processor
        self.forwarder = Forwarder(settings.kafka_brokers)
        self.tiers = [Tier(settings.topic, 0)] + [
            Tier(topic, delay)
            for topic, delay in zip(settings.retry_topics, settings.retry_delays, strict=True)
        ]
        self.delays = {t.topic: t.delay for t in self.tiers}
        self.stopping = threading.Event()
        self.failed = threading.Event()
        self.threads = [
            threading.Thread(target=self._run_tier, args=(tier,), name=f"tier:{tier.topic}")
            for tier in self.tiers
        ]

    def start(self) -> None:
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        self.stopping.set()
        for thread in self.threads:
            thread.join()

    def _run_tier(self, tier: Tier) -> None:
        try:
            self._consume(tier)
        except Exception:
            log.exception("tier %s crashed; stopping the worker", tier.topic)
            self.failed.set()
            self.stopping.set()

    def _consume(self, tier: Tier) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self.settings.kafka_brokers,
                "group.id": self.settings.group_id,
                # Offsets are stored only once a message is handled, then committed in the
                # background; per-message commits crowd out heartbeats under load.
                "enable.auto.commit": True,
                "enable.auto.offset.store": False,
                "auto.commit.interval.ms": 1000,
                "auto.offset.reset": "earliest",
                # A delay tier holds its head message for up to `delay` between polls.
                "max.poll.interval.ms": int(max(300, tier.delay + 60) * 1000),
            }
        )
        consumer.subscribe([tier.topic])
        backoff = 0.5
        try:
            while not self.stopping.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    log.warning("consumer error on %s: %s", tier.topic, msg.error())
                    continue
                if not self._wait_until_due(msg):
                    break  # shutting down; the uncommitted message is redelivered later
                try:
                    self._handle(msg)
                except (psycopg.OperationalError, KafkaException) as e:
                    # Not the event's fault: put it back and try again shortly.
                    log.error(
                        "could not handle a message from %s, retrying in %.1fs: %s", tier.topic, backoff, e
                    )
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    self.stopping.wait(backoff)
                    backoff = min(backoff * 2, 30)
                    continue
                consumer.store_offsets(message=msg)
                backoff = 0.5
        finally:
            consumer.close()

    def _wait_until_due(self, msg: Message) -> bool:
        due = int(_header(msg, NOT_BEFORE) or 0) / 1000
        remaining = due - time.time()
        return remaining <= 0 or not self.stopping.wait(remaining)

    def _handle(self, msg: Message) -> None:
        attempt = int(_header(msg, ATTEMPT) or 1)
        try:
            event = Event.parse(msg.value())
        except PermanentError as e:
            log.error("unreadable message at %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), e)
            self.forwarder.send(self.settings.dead_letter_topic, msg, attempt, f"PermanentError: {e}")
            return

        outcome = self.processor.process(event, attempt)
        if outcome.status == "retry":
            self.forwarder.send(
                outcome.next_topic, msg, attempt + 1, outcome.error, self.delays[outcome.next_topic]
            )
        elif outcome.status == "dead":
            self.forwarder.send(outcome.next_topic, msg, attempt, outcome.error)


def _header(msg: Message, name: str) -> str | None:
    for key, value in msg.headers() or []:
        if key == name and value is not None:
            return value.decode()
    return None
