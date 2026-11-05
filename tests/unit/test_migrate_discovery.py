"""Migration discovery (spec 7.3): file names, order, validation and the lock wait. No database."""

import contextlib
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

import pytest

from langgraph_and_observability.persistence import migrate
from langgraph_and_observability.persistence.migrate import Migration, discover, run_migrate
from langgraph_and_observability.settings import load_settings

# Port 1 refuses connections: a test that reaches the database fails loudly.
UNREACHABLE = "postgresql://u@127.0.0.1:1/x"


def _write(directory: Path, filename: str, sql: str = "SELECT 1;") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / filename).write_text(sql, encoding="utf-8")


def test_discover_orders_and_validates(tmp_path):
    core, worker = tmp_path / "core", tmp_path / "worker"
    # Written out of order; README.md is not a migration and is ignored.
    _write(core, "0002_second.sql", "CREATE TABLE b ();")
    _write(core, "0001_first.sql", "CREATE TABLE a ();")
    _write(core, "README.md", "notes")
    _write(worker, "0001_runs.sql", "CREATE TABLE runs ();")

    found = discover([("core", core), ("worker", worker), ("absent", tmp_path / "absent")])

    assert found == [
        Migration("core", 1, "first", "CREATE TABLE a ();", True),
        Migration("core", 2, "second", "CREATE TABLE b ();", True),
        Migration("worker", 1, "runs", "CREATE TABLE runs ();", True),
    ]

    _write(tmp_path / "dup", "0001_a.sql")
    _write(tmp_path / "dup", "0001_b.sql")
    with pytest.raises(ValueError, match="duplicate"):
        discover([("dup", tmp_path / "dup")])

    _write(tmp_path / "gap", "0001_a.sql")
    _write(tmp_path / "gap", "0003_c.sql")
    with pytest.raises(ValueError, match="gap"):
        discover([("gap", tmp_path / "gap")])


def test_discover_rejects_bad_names_and_repeated_components(tmp_path):
    _write(tmp_path / "zero", "0000_init.sql")
    with pytest.raises(ValueError, match="start at 1"):
        discover([("zero", tmp_path / "zero")])

    _write(tmp_path / "bad", "1_init.sql")
    with pytest.raises(ValueError, match="NNNN_name.sql"):
        discover([("bad", tmp_path / "bad")])

    _write(tmp_path / "ok", "0001_init.sql")
    with pytest.raises(ValueError, match="more than once"):
        discover([("core", tmp_path / "ok"), ("core", tmp_path / "ok")])


def test_no_transaction_marker(tmp_path):
    _write(
        tmp_path,
        "0001_index.sql",
        "-- migrate:no-transaction\nCREATE INDEX CONCURRENTLY i ON t (x);\n",
    )
    _write(tmp_path, "0002_table.sql", "-- an ordinary comment\nCREATE TABLE u ();\n")

    index, table = discover([("core", tmp_path)])

    assert (index.name, index.transactional) == ("index", False)
    assert (table.name, table.transactional) == ("table", True)


async def test_run_migrate_checks_its_arguments_before_connecting(tmp_path):
    settings = load_settings(database_url=UNREACHABLE)
    with pytest.raises(ValueError, match="unknown components: wroker"):
        await run_migrate(settings, components={"core", "wroker"})
    with pytest.raises(ValueError, match="reserved"):
        await run_migrate(settings, agent_sources=[("langgraph", tmp_path)])
    with pytest.raises(ValueError, match="APP_MIGRATION_DATABASE_URL or APP_DATABASE_URL"):
        await run_migrate(load_settings())


class _Connection:
    """A connection that records its statements; the lock calls are faked separately."""

    broken = False

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, query: str, params: object = None) -> None:
        self.statements.append(query)


class _Pool:
    def __init__(self) -> None:
        self.conn = _Connection()

    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[_Connection]:
        yield self.conn


async def test_waiting_for_the_lock_logs_about_every_ten_seconds(monkeypatch, caplog):
    # Another runner holds the lock for 45 polls (22.5 s at 0.5 s a poll): two lines,
    # one about every 10 s of waiting, and none once the lock is taken.
    answers = iter([False] * 45 + [True])

    async def try_lock(conn):
        return next(answers)

    monkeypatch.setattr(migrate, "_try_lock", try_lock)
    monkeypatch.setattr(migrate, "_LOCK_POLL_S", 0)
    pool = _Pool()

    with caplog.at_level(logging.INFO, logger=migrate.__name__):
        async with migrate._migration_lock(cast(Any, pool)):
            pass

    waiting = [r for r in caplog.records if r.getMessage() == "migrate.waiting_for_lock"]
    assert [r.levelname for r in waiting] == ["WARNING", "WARNING"]
    assert next(answers, None) is None
    assert any("pg_advisory_unlock" in statement for statement in pool.conn.statements)
