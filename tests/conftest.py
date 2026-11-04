"""Shared pytest fixtures for the test suite."""

import os
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
from psycopg import AsyncConnection, sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.persistence.migrate import run_migrate
from langgraph_postgres_starter.persistence.pool import create_pool, open_pool
from langgraph_postgres_starter.persistence.resources import set_pool
from langgraph_postgres_starter.settings import get_settings, load_settings

PG_IMAGE = "pgvector/pgvector:0.8.4-pg18"

# Read once, at import: _isolate_settings removes every APP_* variable before each test.
TEST_DATABASE_URL = os.environ.get("APP_TEST_DATABASE_URL") or None


@pytest.fixture(autouse=True)
def _isolate_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Hide the developer's APP_* and DD_* variables and reset the cached settings.

    Settings matches names case-insensitively, so the check does too.
    """
    for name in list(os.environ):
        if name.upper().startswith(("APP_", "DD_")):
            monkeypatch.delenv(name)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(params=[False, True], ids=["permissive-msgpack", "strict-msgpack"])
def strict_msgpack(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> bool:
    """Run the test with LANGGRAPH_STRICT_MSGPACK off, then on (the image turns it on).

    LangGraph reads the variable once, at import, into two module constants: the
    serializer's default allowlist reads one, and StateGraph.compile() the other. The
    fixture sets the variable and patches both constants.
    """
    # Imported here: both modules are private. If an upgrade moves one, only the tests
    # that use this fixture fail.
    import langgraph._internal._serde
    import langgraph.checkpoint.serde._msgpack

    strict: bool = request.param
    monkeypatch.setenv("LANGGRAPH_STRICT_MSGPACK", str(strict).lower())
    monkeypatch.setattr(langgraph.checkpoint.serde._msgpack, "STRICT_MSGPACK_ENABLED", strict)
    monkeypatch.setattr(langgraph._internal._serde, "STRICT_MSGPACK_ENABLED", strict)
    return strict


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


@pytest.fixture
async def steps_pool(
    pool: AsyncConnectionPool[AsyncConnection[DictRow]], fresh_dsn: str
) -> AsyncIterator[AsyncConnectionPool[AsyncConnection[DictRow]]]:
    """The test pool, ready to run the ``test_steps`` graph under the worker.

    Applies the core, langgraph and worker migrations, creates ``test_step_calls`` and
    registers the pool with ``set_pool``, so the steps record their marks in Postgres.
    Afterwards it clears the registered pool and ``tests.graphs.steps.FAULTS``.
    """
    # Imported here, so that loading this conftest does not import the test-only graph.
    # Always as tests.graphs.steps: the tests change this module's FAULTS.
    from tests.graphs import steps

    await run_migrate(
        load_settings(database_url=fresh_dsn), components={"core", "langgraph", "worker"}
    )
    await steps.ensure_table(pool)
    set_pool(pool)
    try:
        yield pool
    finally:
        set_pool(None)
        steps.FAULTS.clear()
