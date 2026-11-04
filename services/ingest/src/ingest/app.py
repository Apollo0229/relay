from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, Path, Request, Response
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field

from .settings import Settings

SOURCE_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"


class EventIn(BaseModel):
    type: str = Field(
        min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_.:-]+$", examples=["payment.succeeded"]
    )
    data: dict[str, Any] = Field(default_factory=dict)


class Accepted(BaseModel):
    id: UUID
    duplicate: bool


class Stats(BaseModel):
    events: int
    pending: int
    published: int


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with AsyncConnectionPool(
            settings.database_url, max_size=settings.pool_size, open=False
        ) as pool:
            await pool.wait()
            app.state.pool = pool
            yield

    app = FastAPI(title="Relay ingest", version="0.1.0", lifespan=lifespan)

    @app.post("/v1/events/{source}", status_code=202)
    async def ingest(
        source: Annotated[str, Path(pattern=SOURCE_PATTERN)],
        event: EventIn,
        request: Request,
        response: Response,
        idempotency_key: Annotated[str | None, Header(max_length=200)] = None,
    ) -> Accepted:
        """Store the event and its outbox message atomically.

        A repeated Idempotency-Key for the same source returns the original
        event with 200 instead of storing it again.
        """
        event_id = uuid4()
        async with request.app.state.pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                """
                insert into events (id, source, type, idempotency_key, payload)
                values (%s, %s, %s, %s, %s)
                on conflict (source, idempotency_key) do nothing
                returning received_at
                """,
                (event_id, source, event.type, idempotency_key, Jsonb(event.data)),
            )
            row = await cur.fetchone()
            if row is None:
                # Only possible with a key: the original was committed first.
                cur = await conn.execute(
                    "select id from events where source = %s and idempotency_key = %s",
                    (source, idempotency_key),
                )
                (existing,) = await cur.fetchone()
                response.status_code = 200
                return Accepted(id=existing, duplicate=True)

            received_at: datetime = row[0]
            envelope = {
                "id": str(event_id),
                "source": source,
                "type": event.type,
                "data": event.data,
                "received_at": received_at.isoformat(),
            }
            # Keyed by source, so a source's events stay in order on one partition.
            await conn.execute(
                "insert into outbox (event_id, topic, key, payload) values (%s, %s, %s, %s)",
                (event_id, settings.topic, source, Jsonb(envelope)),
            )
        return Accepted(id=event_id, duplicate=False)

    @app.get("/v1/stats")
    async def stats(request: Request) -> Stats:
        async with request.app.state.pool.connection() as conn:
            cur = await conn.execute(
                """
                select (select count(*) from events),
                       count(*) filter (where published_at is null),
                       count(*) filter (where published_at is not null)
                from outbox
                """
            )
            events, pending, published = await cur.fetchone()
        return Stats(events=events, pending=pending, published=published)

    @app.get("/healthz")
    async def healthz(request: Request) -> dict[str, str]:
        async with request.app.state.pool.connection() as conn:
            await conn.execute("select 1")
        return {"status": "ok"}

    return app
