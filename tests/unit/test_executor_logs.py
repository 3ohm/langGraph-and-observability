"""The run executor without Postgres: its log context and its stored errors (spec §8.2, §12.1).

The runs repository is a fake that records what the executor reports, and the graph
runs on an in-memory checkpointer.
"""

import dataclasses
import json
import logging
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, StateGraph
from langgraph.runtime import Runtime

from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.logging import configure_logging, get_logger
from langgraph_postgres_starter.settings import load_settings
from langgraph_postgres_starter.worker.executor import RunExecutor
from langgraph_postgres_starter.worker.runs import RunRepo, RunRow, RunStatus
from tests.graphs.steps import STEPS_SPEC, StepsContext, StepsState

OWNER = "pod-a"
AGENT = "logging_steps"


class FakeRepo:
    """Ends every attempt as the pod's own: each outcome is recorded and succeeds."""

    def __init__(self) -> None:
        self.outcomes: list[tuple[str, UUID, Any]] = []

    async def mark_success(self, run_id: UUID, owner: str) -> bool:
        self.outcomes.append(("success", run_id, None))
        return True

    async def mark_error(
        self, run_id: UUID, owner: str, error: dict[str, Any], max_attempts: int
    ) -> RunStatus:
        self.outcomes.append(("error", run_id, error))
        return RunStatus.PENDING


async def _logging_node(state: StepsState, runtime: Runtime[StepsContext]) -> dict[str, list[str]]:
    # A node's own log line, and one from a library or adapter through the logging module.
    get_logger("tests.node").info("node.ran")
    logging.getLogger("tests.adapter").warning("adapter.called")
    return {"log": ["logged"]}


def _build_graph() -> StateGraph[StepsState, StepsContext, StepsState, StepsState]:
    builder = StateGraph(StepsState, context_schema=StepsContext)
    builder.add_node("log", _logging_node)
    builder.add_edge(START, "log")
    return builder


SPEC = dataclasses.replace(STEPS_SPEC, name=AGENT, build_graph=_build_graph)


def _run(run_input: dict[str, Any], attempt: int = 0) -> RunRow:
    return RunRow(
        run_id=new_id(),
        agent=AGENT,
        dedup_key=f"{AGENT}:k",
        status=RunStatus.RUNNING,
        input=run_input,
        attempt=attempt,
        lease_owner=OWNER,
        lease_expires_at=None,
        created_at=datetime.now(UTC),
        started_at=datetime.now(UTC),
        ended_at=None,
        error=None,
        checkpoints_pruned_at=None,
    )


def _executor(repo: FakeRepo) -> RunExecutor:
    return RunExecutor(
        saver=InMemorySaver(),
        agents={AGENT: SPEC},
        repo=cast(RunRepo, repo),
        settings=load_settings(),
        owner=OWNER,
    )


def _lines(text: str) -> dict[str, dict[str, Any]]:
    return {line["event"]: line for line in map(json.loads, text.splitlines())}


async def test_every_log_line_of_a_run_carries_the_run(capsys):
    # Spec §12.1: the logs carry the run. The executor binds the run's ID, agent and
    # attempt for the whole invocation, so every line logged inside it has them, also a
    # node's and a library's, which know nothing of the run.
    configure_logging(load_settings())
    repo = FakeRepo()
    run = _run({"key": "k"}, attempt=1)

    assert await _executor(repo).execute(run) == RunStatus.SUCCESS
    get_logger("tests").info("after.run")

    lines = _lines(capsys.readouterr().out)
    expected = {"run_id": str(run.run_id), "agent": AGENT, "attempt": 2}
    for event in ("run.start", "node.ran", "adapter.called", "run.success"):
        assert {key: lines[event].get(key) for key in expected} == expected, event
    # The context ends with the run.
    assert "run_id" not in lines["after.run"]
    assert [outcome[0] for outcome in repo.outcomes] == ["success"]


async def test_invalid_input_error_holds_no_input_value(capsys):
    configure_logging(load_settings())
    repo = FakeRepo()
    run = _run({"key": {"token": "s3cr3t-value"}})

    assert await _executor(repo).execute(run) == RunStatus.PENDING

    [(outcome, run_id, error)] = repo.outcomes
    assert (outcome, run_id) == ("error", run.run_id)
    assert error["type"] == "ValidationError"
    # The location and the reason of each failure, never the value that failed.
    assert (
        error["message"] == "1 validation error for StepsInput: key: Input should be a valid string"
    )
    out = capsys.readouterr().out
    assert "s3cr3t" not in out
    assert _lines(out)["run.attempt_failed"]["error"] == error
