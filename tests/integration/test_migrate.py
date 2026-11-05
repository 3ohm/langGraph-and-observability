"""The migration runner and the database roles against a real Postgres (spec 7.3; P4)."""

import asyncio
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from langgraph_and_observability.persistence.migrate import (
    CORE_MIGRATIONS_DIR,
    apply,
    discover,
    run_migrate,
)
from langgraph_and_observability.settings import load_settings

pytestmark = pytest.mark.integration

ROLES_SQL = Path(__file__).parents[2] / "deploy" / "sql" / "roles.sql"
CHECKPOINT_TABLES = {"checkpoints", "checkpoint_blobs", "checkpoint_writes"}


async def _existing(dsn: str, *names: str) -> set[str]:
    """Return the subset of ``names`` that exist as relations in the database."""
    async with await psycopg.AsyncConnection.connect(dsn) as c:
        cur = await c.execute(
            "select n from unnest(%s::text[]) n where to_regclass(n) is not null", [list(names)]
        )
        return {row[0] for row in await cur.fetchall()}


async def test_apply_is_idempotent(pool):
    migrations = discover([("core", CORE_MIGRATIONS_DIR)])

    first = await apply(pool, migrations)
    second = await apply(pool, migrations)

    assert [(m.component, m.version, m.name) for m in first] == [("core", 1, "action_ledger")]
    assert second == []
    async with pool.connection() as c:
        rows = await (
            await c.execute("select component, version, name from schema_migrations")
        ).fetchall()
    assert rows == [{"component": "core", "version": 1, "name": "action_ledger"}]


async def test_concurrent_apply_runs_once(pool):
    migrations = discover([("core", CORE_MIGRATIONS_DIR)])

    a, b = await asyncio.gather(apply(pool, migrations), apply(pool, migrations))

    assert len(a) + len(b) == 1


async def test_failed_migration_rolls_back_and_releases_lock(pool, tmp_path):
    (tmp_path / "0001_broken.sql").write_text("CREATE TABLE half_done (x int);\nSELECT 1 / 0;\n")
    migrations = discover([("extra", tmp_path)])

    with pytest.raises(psycopg.errors.DivisionByZero):
        await apply(pool, migrations)

    # The table and the schema_migrations row went back together. No session holds an
    # advisory lock in this database any more (a pooled connection would keep it).
    async with pool.connection() as c:
        assert await (await c.execute("select to_regclass('half_done') as t")).fetchone() == {
            "t": None
        }
        assert await (
            await c.execute("select count(*) as n from schema_migrations")
        ).fetchone() == {"n": 0}
        locks = await c.execute(
            "select count(*) as n from pg_locks where locktype = 'advisory'"
            " and database = (select oid from pg_database where datname = current_database())"
        )
        assert await locks.fetchone() == {"n": 0}


async def test_no_transaction_migration_runs_outside_a_transaction(pool, tmp_path):
    (tmp_path / "0001_table.sql").write_text("CREATE TABLE t (x int);\n")
    # CREATE INDEX CONCURRENTLY fails inside a transaction block.
    (tmp_path / "0002_index.sql").write_text(
        "-- migrate:no-transaction\nCREATE INDEX CONCURRENTLY t_x_idx ON t (x);\n"
    )

    applied = await apply(pool, discover([("extra", tmp_path)]))

    assert [m.version for m in applied] == [1, 2]
    async with pool.connection() as c:
        row = await (
            await c.execute(
                "select indisvalid from pg_index where indexrelid = 't_x_idx'::regclass"
            )
        ).fetchone()
    assert row == {"indisvalid": True}


async def test_langgraph_component_creates_checkpoint_tables(fresh_dsn):
    applied = await run_migrate(load_settings(database_url=fresh_dsn), components={"langgraph"})

    assert applied == []
    assert await _existing(fresh_dsn, *CHECKPOINT_TABLES, "action_ledger") == CHECKPOINT_TABLES


async def test_components_filter_skips_langgraph(fresh_dsn):
    applied = await run_migrate(load_settings(database_url=fresh_dsn), components={"core"})

    assert [(m.component, m.version) for m in applied] == [("core", 1)]
    assert await _existing(fresh_dsn, *CHECKPOINT_TABLES, "action_ledger") == {"action_ledger"}


async def test_run_migrate_orders_core_langgraph_then_agents(fresh_dsn, tmp_path):
    # The agent's migration reads tables from core and from the checkpointer, so it
    # succeeds only if both ran before it.
    (tmp_path / "0001_view.sql").write_text(
        "CREATE VIEW demo AS SELECT key, v FROM action_ledger, checkpoint_migrations;\n"
    )

    applied = await run_migrate(
        load_settings(database_url=fresh_dsn), agent_sources=[("demo", tmp_path)]
    )

    assert applied[0].component == "core" and applied[-1].component == "demo"
    assert await _existing(fresh_dsn, "demo") == {"demo"}


async def test_concurrent_run_migrate_with_checkpointer_setup(fresh_dsn):
    # setup() runs CREATE INDEX CONCURRENTLY while the second run waits for the lock.
    settings = load_settings(database_url=fresh_dsn)

    a, b = await asyncio.gather(run_migrate(settings), run_migrate(settings))

    assert len([m for m in a + b if m.component == "core"]) == 1
    async with await psycopg.AsyncConnection.connect(fresh_dsn) as c:
        invalid = await (
            await c.execute("select count(*) from pg_index where not indisvalid")
        ).fetchone()
    assert invalid == (0,)


def _roles_script(migrator_password: str, app_password: str) -> str:
    """roles.sql with its psql variables filled in, as ``psql -v name=value`` would do.

    psycopg has no psql variables, so this replaces each ``:'name'`` with a quoted
    literal. The test passwords are hex strings, so they need no escaping.
    """
    script = (
        ROLES_SQL.read_text(encoding="utf-8")
        .replace(":'migrator_password'", f"'{migrator_password}'")
        .replace(":'app_password'", f"'{app_password}'")
    )
    assert ":'" not in script
    return script


async def test_app_role_cannot_run_ddl(fresh_dsn):
    migrator_pw, app_pw = uuid.uuid4().hex, uuid.uuid4().hex
    async with await psycopg.AsyncConnection.connect(fresh_dsn, autocommit=True) as admin:
        await admin.execute(_roles_script(migrator_pw, app_pw).encode())
    migrator_dsn = make_conninfo(fresh_dsn, user="lps_migrator", password=migrator_pw)
    app_dsn = make_conninfo(fresh_dsn, user="lps_app", password=app_pw)

    # Every component, including the checkpointer's indexes, migrates as lps_migrator.
    await run_migrate(load_settings(database_url=app_dsn, migration_database_url=migrator_dsn))

    async with await psycopg.AsyncConnection.connect(app_dsn, autocommit=True) as app:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            await app.execute("CREATE TABLE app_made (x int)")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            await app.execute("DROP TABLE action_ledger")
        await app.execute(
            "INSERT INTO action_ledger (key, action, args_hash, result)"
            " VALUES ('k1', 'publish', 'h', '{}'::jsonb)"
        )
        await app.execute("DELETE FROM checkpoint_writes")
