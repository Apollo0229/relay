"""What a worker does with an event, and the event itself."""

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import psycopg


class PermanentError(Exception):
    """The event can never succeed, so retrying it is pointless."""


@dataclass(frozen=True)
class Event:
    id: UUID
    source: str
    type: str
    data: dict[str, Any]

    @classmethod
    def parse(cls, raw: bytes) -> "Event":
        """Reads the envelope the ingest API writes to the outbox."""
        try:
            body = json.loads(raw)
            return cls(id=UUID(body["id"]), source=body["source"], type=body["type"], data=body["data"])
        except (ValueError, TypeError, KeyError) as e:
            raise PermanentError(f"malformed event: {e!r}") from e


def count_event(conn: psycopg.Connection, event: Event, attempt: int) -> None:
    """The demo handler: counts events per source and type.

    `data.simulate` exercises the failure paths: "flaky" fails only its first
    attempt, "error" fails every attempt, and "invalid" fails permanently.
    """
    match event.data.get("simulate"):
        case "invalid":
            raise PermanentError("event marked invalid")
        case "error":
            raise RuntimeError("simulated downstream outage")
        case "flaky" if attempt == 1:
            raise RuntimeError("simulated timeout on the first attempt")

    conn.execute(
        """
        insert into event_counts (source, type, count) values (%s, %s, 1)
        on conflict (source, type) do update set count = event_counts.count + 1
        """,
        (event.source, event.type),
    )
