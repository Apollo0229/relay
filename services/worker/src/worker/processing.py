"""Applies an event once and decides where it goes when it fails. No Kafka here."""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

import psycopg
from psycopg_pool import ConnectionPool

from .handlers import Event, PermanentError

log = logging.getLogger(__name__)

Handler = Callable[[psycopg.Connection, Event, int], None]


@dataclass(frozen=True)
class Outcome:
    status: Literal["done", "duplicate", "retry", "dead"]
    # Where to send the message next: a retry topic, or the dead-letter topic.
    next_topic: str | None = None
    error: str | None = None


class Processor:
    """Runs a handler inside the transaction that records the event as processed.

    The (consumer, event_id) row and the handler's writes commit together, so an
    event's effects are applied exactly once however often Kafka delivers it. Two
    workers given the same event at once serialise on that row's primary key.

    Handler errors are retried on the delay topics, one per attempt, then sent to
    the dead-letter topic; a PermanentError goes there at once. Database
    connection errors are raised instead: the event isn't at fault, so the caller
    should retry it in place.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        consumer: str,
        handler: Handler,
        retry_topics: Sequence[str],
        dead_letter_topic: str,
    ) -> None:
        self.pool = pool
        self.consumer = consumer
        self.handler = handler
        self.retry_topics = list(retry_topics)
        self.dead_letter_topic = dead_letter_topic

    def process(self, event: Event, attempt: int) -> Outcome:
        try:
            with self.pool.connection() as conn, conn.transaction():
                claimed = conn.execute(
                    """
                    insert into processed_events (consumer, event_id, status, attempts)
                    values (%s, %s, 'done', %s)
                    on conflict do nothing
                    """,
                    (self.consumer, event.id, attempt),
                ).rowcount
                if not claimed:
                    return Outcome("duplicate")
                self.handler(conn, event, attempt)
            return Outcome("done")
        except psycopg.OperationalError:
            raise
        except PermanentError as e:
            return self._give_up(event, attempt, e)
        except Exception as e:
            if attempt <= len(self.retry_topics):
                log.warning("event %s failed attempt %d: %s", event.id, attempt, e)
                return Outcome("retry", self.retry_topics[attempt - 1], _describe(e))
            return self._give_up(event, attempt, e)

    def _give_up(self, event: Event, attempt: int, error: Exception) -> Outcome:
        log.error("event %s sent to the dead-letter topic after %d attempt(s): %s", event.id, attempt, error)
        with self.pool.connection() as conn:
            conn.execute(
                """
                insert into processed_events (consumer, event_id, status, attempts, error)
                values (%s, %s, 'dead', %s, %s)
                on conflict do nothing
                """,
                (self.consumer, event.id, attempt, _describe(error)),
            )
        return Outcome("dead", self.dead_letter_topic, _describe(error))


def _describe(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"
