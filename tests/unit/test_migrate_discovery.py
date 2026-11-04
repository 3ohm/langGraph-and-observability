"""Migration discovery (spec 7.3): file names, order and validation. No database."""

from pathlib import Path

import pytest

from langgraph_postgres_starter.persistence.migrate import Migration, discover, run_migrate
from langgraph_postgres_starter.settings import load_settings

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
