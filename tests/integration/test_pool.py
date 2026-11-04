"""The connection pool against a real Postgres (spec 7.1; practices P1 and P2)."""

import psycopg
import pytest

from langgraph_postgres_starter.persistence.pool import (
    DatabaseUnavailable,
    build_conninfo,
    create_pool,
    open_pool,
)
from langgraph_postgres_starter.settings import load_settings

pytestmark = pytest.mark.integration


async def test_autocommit_and_dict_rows(pool):
    async with pool.connection() as c:
        assert c.autocommit and await (await c.execute("select 1 as x")).fetchone() == {"x": 1}


async def test_killed_connection_is_replaced(pool, fresh_dsn):
    async with pool.connection() as c:
        old_pid = c.info.backend_pid

    # Kill the pooled connection's backend from a second connection. The timeout makes
    # pg_terminate_backend wait until the backend has exited.
    async with await psycopg.AsyncConnection.connect(fresh_dsn, autocommit=True) as admin:
        killed = await (
            await admin.execute("select pg_terminate_backend(%s, 5000)", [old_pid])
        ).fetchone()
        assert killed == (True,)

    # The checkout check finds the dead connection, and the pool gives a new one.
    async with pool.connection() as c:
        assert c.info.backend_pid != old_pid
        assert await (await c.execute("select 1 as x")).fetchone() == {"x": 1}


async def test_unreachable_database_fails_fast():
    p = create_pool(load_settings(), dsn="postgresql://u:p@127.0.0.1:1/x")
    with pytest.raises(DatabaseUnavailable) as exc_info:
        await open_pool(p, timeout=3)
    assert isinstance(exc_info.value.__cause__, psycopg.OperationalError)
    assert "u:p@" not in str(exc_info.value) and "password" not in str(exc_info.value)


async def test_pgbouncer_mode_disables_prepared_statements(fresh_dsn):
    p = create_pool(load_settings(pgbouncer_mode=True), dsn=fresh_dsn)
    await open_pool(p, timeout=30)
    try:
        async with p.connection() as c:
            assert c.prepare_threshold is None
            # Run one query more often than the default threshold (5): the server still
            # has no prepared statement, as PgBouncer's transaction mode needs.
            for _ in range(6):
                await c.execute("select %s::int as x", [1])
            row = await (
                await c.execute("select count(*) as n from pg_prepared_statements")
            ).fetchone()
            assert row == {"n": 0}
    finally:
        await p.close()


def test_conninfo_adds_keepalives():
    assert "keepalives=1" in build_conninfo("postgresql://u@h/db", load_settings())
