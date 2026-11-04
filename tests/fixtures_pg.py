"""The Postgres fixtures, for every test directory: pg_dsn, fresh_dsn and pool.

The repo-root conftest.py registers this module as a plugin (``pytest_plugins``), so
the agents' tests under ``src/`` get these fixtures too: pytest applies
``tests/conftest.py`` only to the tests below ``tests/``.
"""

import os
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
from psycopg import AsyncConnection, sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.persistence.pool import create_pool, open_pool
from langgraph_postgres_starter.settings import load_settings

PG_IMAGE = "pgvector/pgvector:0.8.4-pg18"

# Read once, at import: _isolate_settings (repo-root conftest.py) removes every APP_*
# variable before each test.
TEST_DATABASE_URL = os.environ.get("APP_TEST_DATABASE_URL") or None


@pytest.fixture(scope="session")
def pg_dsn() -> Iterator[str]:
    """A Postgres server for the session: APP_TEST_DATABASE_URL, or a new container.

    The container starts on first use only, so a run without integration tests never
    needs Docker. It is removed when the session ends.
    """
    if TEST_DATABASE_URL:
        yield TEST_DATABASE_URL
        return
    # Imported here: unit test runs never load testcontainers.
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(PG_IMAGE, driver=None) as container:
        yield container.get_connection_url()


@pytest.fixture
async def fresh_dsn(pg_dsn: str) -> AsyncIterator[str]:
    """A new, empty database named t_<uuid hex> on the pg_dsn server, dropped afterwards."""
    name = f"t_{uuid.uuid4().hex}"
    async with await AsyncConnection.connect(pg_dsn, autocommit=True) as admin:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield make_conninfo(pg_dsn, dbname=name)
    finally:
        async with await AsyncConnection.connect(pg_dsn, autocommit=True) as admin:
            await admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


@pytest.fixture
async def pool(fresh_dsn: str) -> AsyncIterator[AsyncConnectionPool[AsyncConnection[DictRow]]]:
    """An open pool on fresh_dsn with the default settings, closed after the test."""
    p = create_pool(load_settings(), dsn=fresh_dsn)
    await open_pool(p, timeout=30)
    try:
        yield p
    finally:
        await p.close()
