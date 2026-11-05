"""Worker startup failures and the ``worker`` subcommand (spec 7.1, 13.6).

No test needs Docker: a DSN that points at port 1 on the loopback stands for a
database that is down.
"""

import json
from typing import Any, cast

import pytest

from langgraph_and_observability import __main__
from langgraph_and_observability.persistence.pool import DatabaseUnavailable
from langgraph_and_observability.persistence.resources import current_pool, set_pool
from langgraph_and_observability.settings import load_settings
from langgraph_and_observability.worker import main as worker_main
from langgraph_and_observability.worker.main import EXIT_STARTUP_FAILED, Worker
from tests.graphs.steps import STEPS_SPEC

# Nothing listens on port 1, so every connection attempt is refused at once.
DEAD_DSN = "postgresql://lps:hunter2-secret@127.0.0.1:1/lps"


def _log_lines(text: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in text.splitlines() if line.startswith("{")]


def _startup_failure(text: str) -> dict[str, object]:
    [line] = [entry for entry in _log_lines(text) if entry["event"] == "worker.startup_failed"]
    return line


async def test_start_fails_when_the_intake_agent_is_not_registered():
    settings = load_settings(
        database_url=DEAD_DSN, sqs_queue_url="http://sqs.local/q", intake_agent="nope"
    )
    worker = Worker(
        settings, agents={STEPS_SPEC.name: STEPS_SPEC}, sqs_client=object(), owner="pod-a"
    )

    with pytest.raises(ValueError, match="'nope'"):
        await worker.start()
    await worker.stop()


async def test_stop_closes_tracing_after_a_failed_start(monkeypatch):
    calls: list[object] = []
    client = object()

    def configure(settings):
        calls.append("configure")
        return client

    async def close(tracing):
        calls.append(("close", tracing))

    monkeypatch.setattr(worker_main, "configure_tracing", configure)
    monkeypatch.setattr(worker_main, "close_tracing", close)
    settings = load_settings(database_url=DEAD_DSN, db_connect_timeout_s=1)
    worker = Worker(settings, agents={STEPS_SPEC.name: STEPS_SPEC}, sqs_client=None, owner="pod-a")

    with pytest.raises(DatabaseUnavailable):
        await worker.start()

    # Tracing starts first, and stop() closes it (spec 8.4 step 5).
    assert calls == ["configure", ("close", client)]


class _FailingPool:
    async def close(self) -> None:
        raise RuntimeError("the pool did not close")


async def test_stop_clears_the_pool_and_closes_tracing_when_the_pool_fails(monkeypatch):
    # Spec 8.4 step 5: a pool that fails to close still leaves no registered pool, and
    # the traces are still flushed.
    closed: list[object] = []
    tracing = object()

    async def close(client):
        closed.append(client)

    monkeypatch.setattr(worker_main, "close_tracing", close)
    pool = _FailingPool()
    worker = Worker(load_settings(), agents={}, sqs_client=None, owner="pod-a")
    # The state that start() leaves: an open pool, registered, and a tracing client.
    worker._pool = cast(Any, pool)
    worker._tracing = cast(Any, tracing)
    set_pool(cast(Any, pool))

    with pytest.raises(RuntimeError, match="the pool did not close"):
        await worker.stop()

    assert current_pool() is None
    assert closed == [tracing]


def test_main_exits_when_the_database_is_unreachable(monkeypatch, capsys):
    monkeypatch.setenv("APP_DATABASE_URL", DEAD_DSN)
    monkeypatch.setenv("APP_DB_CONNECT_TIMEOUT_S", "1")

    assert worker_main.main([]) == EXIT_STARTUP_FAILED

    out = capsys.readouterr().out
    failure = _startup_failure(out)
    assert failure["level"] == "error"
    assert "could not connect to Postgres" in str(failure["error"])
    assert "hunter2-secret" not in out
    # Tests hide the developer's LangSmith variables, so the worker builds no client.
    assert '"tracing.' not in out


def test_main_exits_when_prod_secrets_are_missing(monkeypatch, capsys):
    monkeypatch.setenv("APP_ENV", "prod")

    assert worker_main.main([]) == EXIT_STARTUP_FAILED

    failure = _startup_failure(capsys.readouterr().out)
    assert "database_url" in str(failure["error"])
    assert "sqs_queue_url" in str(failure["error"])


def test_main_exits_when_settings_are_invalid(monkeypatch, capsys):
    monkeypatch.setenv("APP_WORKER_CONCURRENCY", "many")

    assert worker_main.main([]) == EXIT_STARTUP_FAILED

    error = _startup_failure(capsys.readouterr().out)["error"]
    assert isinstance(error, dict)
    assert error["type"] == "ValidationError"
    # Where and why, never the value: a setting's value can be a secret.
    assert error["message"] == (
        "1 validation error for Settings: worker_concurrency: "
        "Input should be a valid integer, unable to parse string as an integer"
    )
    assert "many" not in error["message"]


def test_cli_worker_subcommand_runs_worker_main(monkeypatch):
    calls = []

    def fake_main(argv):
        calls.append(argv)
        return 7

    monkeypatch.setattr(worker_main, "main", fake_main)

    assert __main__.main(["worker"]) == 7
    assert calls == [[]]
