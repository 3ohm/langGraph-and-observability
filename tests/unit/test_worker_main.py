"""Worker startup failures and the ``worker`` subcommand (spec 7.1, 13.6).

No test needs Docker: a DSN that points at port 1 on the loopback stands for a
database that is down.
"""

import dataclasses
import importlib
import importlib.util
import json
from typing import Any, cast

import pytest

from langgraph_and_observability import __main__
from langgraph_and_observability.persistence.pool import DatabaseUnavailable
from langgraph_and_observability.persistence.resources import current_pool, set_pool
from langgraph_and_observability.settings import MissingSecretsError, load_settings
from langgraph_and_observability.worker import main as worker_main
from langgraph_and_observability.worker.main import (
    EXIT_STARTUP_FAILED,
    Worker,
    check_agent_settings,
)
from tests.graphs.steps import STEPS_SPEC

# Nothing listens on port 1, so every connection attempt is refused at once.
DEAD_DSN = "postgresql://lps:hunter2-secret@127.0.0.1:1/lps"

# The example agent's spec module; scripts/remove_example.py deletes it.
EXAMPLE_SPEC = "langgraph_and_observability.agents.incident_triage.spec"

# An agent that needs two settings in prod. Both are None by default.
NEEDS_SETTINGS = dataclasses.replace(
    STEPS_SPEC,
    name="needs_settings",
    required_settings=lambda settings: ["result_queue_url", "migration_database_url"],
)


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
    # Named here: remove_example.py makes the default empty, and then SQS is not needed.
    monkeypatch.setenv("APP_INTAKE_AGENT", "my_agent")

    assert worker_main.main([]) == EXIT_STARTUP_FAILED

    failure = _startup_failure(capsys.readouterr().out)
    assert "database_url" in str(failure["error"])
    assert "sqs_queue_url" in str(failure["error"])


def test_check_agent_settings_names_what_the_agents_need_in_prod():
    # Two agents need result_queue_url; the error names it once.
    also_needs = dataclasses.replace(
        STEPS_SPEC, name="also_needs", required_settings=lambda settings: ["result_queue_url"]
    )
    agents = {spec.name: spec for spec in (STEPS_SPEC, NEEDS_SETTINGS, also_needs)}

    with pytest.raises(MissingSecretsError) as e:
        check_agent_settings(load_settings(env="prod"), agents)
    assert e.value.missing == ["result_queue_url", "migration_database_url"]

    with pytest.raises(MissingSecretsError) as e:
        check_agent_settings(load_settings(env="prod", result_queue_url="https://sqs/r"), agents)
    assert e.value.missing == ["migration_database_url"]

    check_agent_settings(
        load_settings(
            env="prod", result_queue_url="https://sqs/r", migration_database_url="postgresql://m"
        ),
        agents,
    )


@pytest.mark.parametrize("env", ["dev", "test"])
def test_check_agent_settings_is_quiet_outside_prod(env):
    check_agent_settings(load_settings(env=env), {NEEDS_SETTINGS.name: NEEDS_SETTINGS})


@pytest.mark.skipif(
    importlib.util.find_spec(EXAMPLE_SPEC.rpartition(".")[0]) is None,
    reason="the example agent was removed",
)
def test_check_agent_settings_names_the_example_agents_datadog_keys():
    # Imported by name: the module is gone, and mypy cannot see it, after the removal.
    example = importlib.import_module(EXAMPLE_SPEC).spec
    agents = {example.name: example}

    with pytest.raises(MissingSecretsError) as e:
        check_agent_settings(load_settings(env="prod", adapters="real"), agents)
    assert e.value.missing == ["dd_api_key", "dd_app_key"]

    # Simulated adapters need no keys, and outside prod nothing is checked.
    check_agent_settings(load_settings(env="prod", adapters="simulated"), agents)
    check_agent_settings(load_settings(env="dev", adapters="real"), agents)


def test_main_exits_when_an_agent_setting_is_missing(monkeypatch, capsys):
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("APP_DATABASE_URL", DEAD_DSN)
    monkeypatch.setenv("APP_DB_CONNECT_TIMEOUT_S", "1")
    monkeypatch.setenv("APP_SQS_QUEUE_URL", "http://sqs.local/q")
    monkeypatch.setattr(
        worker_main, "load_agents", lambda settings: {NEEDS_SETTINGS.name: NEEDS_SETTINGS}
    )

    assert worker_main.main([]) == EXIT_STARTUP_FAILED

    # The same log line and exit code as a missing core secret.
    out = capsys.readouterr().out
    failure = _startup_failure(out)
    assert failure["level"] == "error"
    assert failure["error"] == {
        "type": "MissingSecretsError",
        "message": "settings required when env='prod' are not set: "
        "result_queue_url, migration_database_url",
    }
    assert "hunter2-secret" not in out


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
