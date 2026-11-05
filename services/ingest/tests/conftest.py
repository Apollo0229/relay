import asyncio
import os
import selectors
import sys

import httpx
import psycopg
import pytest

from ingest.app import create_app
from ingest.settings import Settings

DATABASE_URL = os.environ.get("RELAY_DATABASE_URL", "postgresql://relay:relay@localhost:5432/relay")


if sys.platform == "win32":
    # psycopg's async mode can't use Windows' default Proactor loop. Defined only here because
    # pytest-asyncio rejects a hook that returns nothing; Linux (CI, production) keeps the default loop.
    def pytest_asyncio_loop_factories(config, item):
        return {"selector": lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())}


@pytest.fixture
def db():
    """A direct connection for assertions; tables are emptied before each test."""
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        conn.execute("truncate outbox, events restart identity")
        yield conn


@pytest.fixture
async def client(db):
    app = create_app(Settings(database_url=DATABASE_URL, pool_size=20))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c
