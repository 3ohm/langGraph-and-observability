"""The operator CLI against a real Postgres (spec 7.3, 8.6).

The tests call the entry point of ``python -m langgraph_and_observability`` in this
process. The CLI runs its own event loop, so the tests are synchronous and set up
their data with ``asyncio.run``.
"""

import asyncio
import dataclasses
import json
import uuid
from datetime import datetime
from typing import Any

import pytest

from langgraph_and_observability.__main__ import main
from langgraph_and_observability.persistence.migrate import run_migrate
from langgraph_and_observability.persistence.pool import create_pool, open_pool
from langgraph_and_observability.settings import load_settings
from langgraph_and_observability.worker import registry
from langgraph_and_observability.worker.runs import RunRepo, RunRow, RunStatus
from tests.graphs.steps import STEPS_SPEC

pytestmark = pytest.mark.integration

AGENT = "test_steps"
INPUT = {"service": "checkout"}
ERROR = {"type": "RuntimeError", "message": "step b failed"}
OWNER = "pod-a"


def _lines(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines()]


async def _seed(dsn: str, *, failed: int = 0, pending: int = 0) -> list[uuid.UUID]:
    """Migrate ``dsn`` and add runs: ``failed`` failed ones, then ``pending`` pending ones."""
    settings = load_settings(database_url=dsn)
    await run_migrate(settings, components={"core", "worker"})
    pool = create_pool(settings)
    await open_pool(pool, timeout=30)
    try:
        repo = RunRepo(pool)
        ids = []
        for i in range(failed + pending):
            run_id, _ = await repo.create(AGENT, f"alert:{i}", INPUT)
            if i < failed:
                assert await repo.claim(run_id, OWNER, 60) is not None
                assert await repo.mark_error(run_id, OWNER, ERROR, 1) is RunStatus.FAILED
            ids.append(run_id)
        return ids
    finally:
        await pool.close()


async def _status(dsn: str, run_id: uuid.UUID) -> tuple[str, int]:
    pool = create_pool(load_settings(database_url=dsn))
    await open_pool(pool, timeout=30)
    try:
        run = await RunRepo(pool).get(run_id)
    finally:
        await pool.close()
    assert run is not None
    return run.status, run.attempt


async def _all_runs(dsn: str) -> list[RunRow]:
    pool = create_pool(load_settings(database_url=dsn))
    await open_pool(pool, timeout=30)
    try:
        return await RunRepo(pool).list()
    finally:
        await pool.close()


@pytest.fixture
def db(fresh_dsn, monkeypatch):
    """APP_DATABASE_URL set to the test database."""
    monkeypatch.setenv("APP_DATABASE_URL", fresh_dsn)
    return fresh_dsn


@pytest.fixture
def steps_agent(monkeypatch):
    """The test_steps agent registered, as the only built-in agent."""
    monkeypatch.setattr(registry, "builtin_agents", lambda: [STEPS_SPEC])


def test_cli_runs_show_prints_json(db, capsys):
    [run_id] = asyncio.run(_seed(db, failed=1))

    assert main(["runs", "show", str(run_id)]) == 0

    out = capsys.readouterr()
    [run] = _lines(out.out)
    assert out.err == ""
    assert {k: run[k] for k in ("run_id", "agent", "dedup_key", "status", "input", "attempt")} == {
        "run_id": str(run_id),
        "agent": AGENT,
        "dedup_key": "alert:0",
        "status": "failed",
        "input": INPUT,
        "attempt": 1,
    }
    assert run["error"] == ERROR
    assert run["lease_owner"] is None
    # Datetimes are ISO 8601 strings.
    for key in ("created_at", "started_at", "ended_at"):
        assert datetime.fromisoformat(run[key]).tzinfo is not None
        assert "T" in run[key]


def test_cli_runs_show_unknown_run_exits_1(db, capsys):
    asyncio.run(_seed(db))
    missing = uuid.uuid7()

    assert main(["runs", "show", str(missing)]) == 1

    out = capsys.readouterr()
    assert out.out == ""
    assert _lines(out.err) == [{"error": "not found", "run_id": str(missing)}]


def test_cli_runs_list_filters_and_limits(db, capsys):
    failed = asyncio.run(_seed(db, failed=2, pending=1))

    assert main(["runs", "list", "--status", "failed"]) == 0
    listed = _lines(capsys.readouterr().out)
    assert main(["runs", "list", "--limit", "1"]) == 0
    limited = _lines(capsys.readouterr().out)

    # Newest first.
    assert [r["run_id"] for r in listed] == [str(i) for i in reversed(failed[:2])]
    assert {r["status"] for r in listed} == {"failed"}
    assert len(limited) == 1
    assert limited[0]["status"] == "pending"


def test_cli_runs_retry_returns_failed_run_to_pending(db, capsys):
    [run_id] = asyncio.run(_seed(db, failed=1))

    assert main(["runs", "retry", str(run_id)]) == 0

    assert _lines(capsys.readouterr().out) == [{"run_id": str(run_id), "status": "pending"}]
    assert asyncio.run(_status(db, run_id)) == ("pending", 0)


def test_cli_runs_retry_refuses_run_that_is_not_failed(db, capsys):
    [run_id] = asyncio.run(_seed(db, pending=1))
    missing = uuid.uuid7()

    assert main(["runs", "retry", str(run_id)]) == 1
    not_retryable = capsys.readouterr()
    assert main(["runs", "retry", str(missing)]) == 1
    not_found = capsys.readouterr()

    assert (not_retryable.out, not_found.out) == ("", "")
    assert _lines(not_retryable.err) == [{"error": "not retryable", "run_id": str(run_id)}]
    assert _lines(not_found.err) == [{"error": "not found", "run_id": str(missing)}]
    assert asyncio.run(_status(db, run_id)) == ("pending", 0)


def test_runs_create_adds_a_pending_run(db, steps_agent, capsys):
    asyncio.run(_seed(db))

    assert main(["runs", "create", AGENT, "--input", json.dumps({"key": "k1"})]) == 0

    out = capsys.readouterr()
    assert out.err == ""
    [line] = _lines(out.out)
    [run] = asyncio.run(_all_runs(db))
    # Without --dedup-key, the agent's dedup_key gives the key.
    assert line == {"run_id": str(run.run_id), "created": True, "dedup_key": "test_steps:k1"}
    assert (run.agent, run.dedup_key, run.status, run.input) == (
        AGENT,
        "test_steps:k1",
        "pending",
        {"key": "k1"},
    )


def test_runs_create_twice_is_one_run(db, steps_agent, capsys):
    asyncio.run(_seed(db))

    def create(key: str) -> dict[str, Any]:
        argv = ["runs", "create", AGENT, "--input", json.dumps({"key": key})]
        assert main([*argv, "--dedup-key", "manual:1"]) == 0
        [line] = _lines(capsys.readouterr().out)
        return line

    # The agent's dedup_key would give two keys; --dedup-key gives one.
    first, second = create("k1"), create("k2")

    assert first["created"] is True
    assert first["dedup_key"] == "manual:1"
    assert second == {**first, "created": False}
    [run] = asyncio.run(_all_runs(db))
    assert (str(run.run_id), run.input) == (first["run_id"], {"key": "k1"})


def test_runs_create_rejects_bad_input_without_its_values(db, steps_agent, capsys):
    asyncio.run(_seed(db))
    # key must be a string; the value that fails holds a secret.
    bad = json.dumps({"key": {"token": "sk-live-SECRET123"}})

    assert main(["runs", "create", AGENT, "--input", bad]) == 1

    out = capsys.readouterr()
    assert out.out == ""
    assert "SECRET123" not in out.err
    # Where each failure is and why, never the value that failed.
    assert _lines(out.err) == [
        {"error": "invalid input", "details": ["key: Input should be a valid string"]}
    ]
    assert asyncio.run(_all_runs(db)) == []


def test_runs_create_unknown_agent(db, steps_agent, capsys):
    asyncio.run(_seed(db))

    assert main(["runs", "create", "nope", "--input", json.dumps({"key": "k1"})]) == 1

    out = capsys.readouterr()
    assert out.out == ""
    assert _lines(out.err) == [{"error": "unknown agent", "agent": "nope"}]
    assert asyncio.run(_all_runs(db)) == []


def test_runs_create_input_that_is_not_json_is_a_usage_error(capsys):
    with pytest.raises(SystemExit) as exited:
        main(["runs", "create", AGENT, "--input", "{'token': 'sk-live-SECRET123'}"])

    assert exited.value.code == 2
    err = capsys.readouterr().err
    assert "--input: not valid JSON" in err
    assert "SECRET123" not in err


@pytest.mark.parametrize("key", ["", "  "], ids=["empty", "blank"])
def test_runs_create_empty_dedup_key_is_a_usage_error(capsys, key):
    # A key from an unset variable (--dedup-key "$KEY") would fold every later run into the first.
    argv = ["runs", "create", AGENT, "--input", json.dumps({"key": "k1"}), "--dedup-key", key]
    with pytest.raises(SystemExit) as exited:
        main(argv)

    assert exited.value.code == 2
    assert "--dedup-key: must not be empty" in capsys.readouterr().err


def test_cli_migrate_components_core(db, capsys):
    assert main(["migrate", "--components", "core"]) == 0
    first = _lines(capsys.readouterr().out)
    assert main(["migrate", "--components", "core"]) == 0
    second = _lines(capsys.readouterr().out)

    assert first == [{"component": "core", "version": 1, "name": "action_ledger"}]
    assert second == []


def test_cli_migrate_all_reports_langgraph_setup_in_run_order(db, capsys):
    assert main(["migrate"]) == 0

    lines = _lines(capsys.readouterr().out)
    assert lines[:4] == [
        {"component": "core", "version": 1, "name": "action_ledger"},
        {"component": "langgraph", "step": "setup"},
        {"component": "worker", "version": 1, "name": "runs"},
        {"component": "worker", "version": 2, "name": "runs_ended_at_index"},
    ]
    # Then the migrations of the built-in agents that have tables.
    with_tables = {spec.name for spec in registry.builtin_agents() if spec.migrations_dir}
    assert {line["component"] for line in lines[4:]} == with_tables


def test_cli_migrate_passes_agent_migrations(db, capsys, tmp_path, monkeypatch):
    (tmp_path / "0001_step_notes.sql").write_text("CREATE TABLE step_notes (id int);\n")
    with_tables = dataclasses.replace(STEPS_SPEC, name="with_tables", migrations_dir=tmp_path)
    without_tables = dataclasses.replace(STEPS_SPEC, name="without_tables")
    monkeypatch.setattr(registry, "builtin_agents", lambda: [with_tables, without_tables])

    assert main(["migrate", "--components", "langgraph,with_tables"]) == 0

    assert _lines(capsys.readouterr().out) == [
        {"component": "langgraph", "step": "setup"},
        {"component": "with_tables", "version": 1, "name": "step_notes"},
    ]


def test_cli_migrate_unknown_component_exits_2(db, capsys):
    assert main(["migrate", "--components", "core,nope"]) == 2

    out = capsys.readouterr()
    assert out.out == ""
    [error] = _lines(out.err)
    assert "unknown components: nope" in error["error"]


@pytest.mark.parametrize("argv", [["runs", "list"], ["migrate"]], ids=["runs", "migrate"])
def test_cli_without_database_url_exits_2(argv, capsys):
    assert main(argv) == 2

    out = capsys.readouterr()
    assert out.out == ""
    [error] = _lines(out.err)
    assert "no database URL" in error["error"]


@pytest.mark.parametrize(
    "argv", [["runs", "show", str(uuid.uuid7())], ["migrate"]], ids=["runs", "migrate"]
)
def test_cli_with_unreachable_database_exits_2_without_the_dsn(argv, capsys, monkeypatch):
    monkeypatch.setenv("APP_DATABASE_URL", "postgresql://lps:hunter2-secret@127.0.0.1:1/lps")
    monkeypatch.setenv("APP_DB_CONNECT_TIMEOUT_S", "1")

    assert main(argv) == 2

    out = capsys.readouterr()
    assert out.out == ""
    assert "hunter2-secret" not in out.err
    assert "Traceback" not in out.err
    [error] = _lines(out.err)
    assert "could not connect to Postgres" in error["error"]
