import os

import pytest
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get("WORKER_DATABASE_URL", "postgresql://relay:relay@localhost:5432/relay")


@pytest.fixture
def pool():
    """A pool on the test database, with the worker's tables emptied."""
    with ConnectionPool(DATABASE_URL, min_size=1, max_size=8, open=True) as pool:
        with pool.connection() as conn:
            conn.execute("truncate processed_events, event_counts")
        yield pool


def counts(pool) -> dict[tuple[str, str], int]:
    with pool.connection() as conn:
        return {(s, t): n for s, t, n in conn.execute("select source, type, count from event_counts")}


def processed(pool) -> list[tuple[str, int, str | None]]:
    with pool.connection() as conn:
        rows = conn.execute("select status, attempts, error from processed_events order by processed_at")
        return rows.fetchall()
