"""The run executor against a real Postgres (spec 8.2-8.5; P11, P14)."""

import asyncio
import time
from collections import Counter
from uuid import UUID

import pytest
import structlog
from langgraph.graph.state import CompiledStateGraph

from langgraph_and_observability.persistence.checkpointer import make_checkpointer
from langgraph_and_observability.persistence.serde import make_serde
from langgraph_and_observability.settings import load_settings
from langgraph_and_observability.worker.executor import RunExecutor
from langgraph_and_observability.worker.runs import RunRepo, RunRow, RunStatus
from tests.graphs import steps
from tests.graphs.steps import STEPS_SPEC, Fault

pytestmark = pytest.mark.integration

OWNER = "pod-a"
LEASE_S = 60


@pytest.fixture
def repo(steps_pool):
    return RunRepo(steps_pool)


@pytest.fixture
async def saver(steps_pool):
    # Async: the saver keeps the running event loop.
    return make_checkpointer(steps_pool, make_serde(STEPS_SPEC.serde_types))


@pytest.fixture
def make_executor(saver, repo):
    """Build a RunExecutor for the steps agent; keyword arguments override settings."""

    def make(metadata=None, **settings):
        return RunExecutor(
            saver=saver,
            agents={STEPS_SPEC.name: STEPS_SPEC},
            repo=repo,
            settings=load_settings(**settings),
            owner=OWNER,
            metadata=metadata,
        )

    return make


async def _claimed(repo: RunRepo, key: str, lease_s: float = LEASE_S) -> RunRow:
    """Create a steps run for ``key`` and claim it for OWNER, with a lease of ``lease_s``."""
    run_id, created = await repo.create(STEPS_SPEC.name, f"test_steps:{key}", {"key": key})
    assert created
    run = await repo.claim(run_id, OWNER, lease_s)
    assert run is not None
    return run


async def _get(repo: RunRepo, run_id: UUID) -> RunRow:
    run = await repo.get(run_id)
    assert run is not None
    return run


async def _marks(pool, run_id: UUID) -> Counter[str]:
    """Count the run's marks. Marks are ordered only by time, so tests compare counts."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT mark FROM test_step_calls WHERE run_id = %s", (str(run_id),)
        )
        return Counter(row["mark"] for row in await cur.fetchall())


async def _wait_for_mark(pool, run_id: UUID, mark: str) -> None:
    """Poll test_step_calls until the run has recorded ``mark``; at most 10 s."""
    async with asyncio.timeout(10):
        while True:
            if (await _marks(pool, run_id))[mark]:
                return
            await asyncio.sleep(0.02)


async def _log(saver, run_id: UUID) -> list[str]:
    """The ``log`` in the run's latest checkpoint."""
    graph = STEPS_SPEC.build_graph().compile(checkpointer=saver)
    state = await graph.aget_state({"configurable": {"thread_id": str(run_id)}})
    log: list[str] = state.values["log"]
    return log


async def test_run_completes(make_executor, repo, saver):
    executor = make_executor()
    run = await _claimed(repo, "ok")

    status = await executor.execute(run)

    assert status == RunStatus.SUCCESS
    row = await _get(repo, run.run_id)
    assert (row.status, row.lease_owner, row.attempt, row.error) == (
        RunStatus.SUCCESS,
        None,
        0,
        None,
    )
    assert await _log(saver, run.run_id) == ["a", "b", "c"]
    assert executor.active_run_ids() == []


async def test_resume_skips_completed_steps(make_executor, repo, saver, steps_pool):
    executor = make_executor()
    steps.FAULTS["resume"] = Fault(fail_step="b", kind="fatal")
    run = await _claimed(repo, "resume")

    assert await executor.execute(run) == RunStatus.PENDING
    failed = await _get(repo, run.run_id)
    assert (failed.status, failed.lease_owner, failed.attempt) == (RunStatus.PENDING, None, 1)
    assert failed.error == {
        "type": "ValueError",
        "message": "injected fatal fault in step b (attempt 1)",
    }

    del steps.FAULTS["resume"]
    resumed = await repo.claim(run.run_id, OWNER, LEASE_S)
    assert resumed is not None
    assert await executor.execute(resumed) == RunStatus.SUCCESS

    # P11: the second invocation resumes at b; a is not repeated.
    assert await _marks(steps_pool, run.run_id) == Counter(
        {"a:start": 1, "a:done": 1, "b:start": 2, "b:done": 1, "c:start": 1, "c:done": 1}
    )
    assert await _log(saver, run.run_id) == ["a", "b", "c"]


async def test_last_attempt_fails_the_run(make_executor, repo):
    # Spec 8.2: after APP_MAX_ATTEMPTS attempts the run is failed, with the error recorded.
    executor = make_executor(max_attempts=1)
    steps.FAULTS["fatal"] = Fault(fail_step="b", kind="fatal")
    run = await _claimed(repo, "fatal")

    with structlog.testing.capture_logs() as logs:
        status = await executor.execute(run)

    assert status == RunStatus.FAILED
    row = await _get(repo, run.run_id)
    assert (row.status, row.lease_owner, row.lease_expires_at, row.attempt) == (
        RunStatus.FAILED,
        None,
        None,
        1,
    )
    assert row.ended_at is not None
    error = {"type": "ValueError", "message": "injected fatal fault in step b (attempt 1)"}
    assert row.error == error
    [failed] = [entry for entry in logs if entry["event"] == "run.failed"]
    assert failed["log_level"] == "error"
    assert (failed["run_id"], failed["attempt"], failed["error"]) == (str(run.run_id), 1, error)
    assert executor.active_run_ids() == []


async def test_deadline_exceeded_returns_pending_with_error(make_executor, repo, steps_pool):
    executor = make_executor(run_deadline_seconds=1)
    steps.FAULTS["slow"] = Fault(delay_s={"b": 5})
    run = await _claimed(repo, "slow")

    started = time.monotonic()
    status = await executor.execute(run)

    assert time.monotonic() - started < 4
    assert status == RunStatus.PENDING
    row = await _get(repo, run.run_id)
    assert (row.status, row.lease_owner, row.attempt) == (RunStatus.PENDING, None, 1)
    assert row.error is not None
    assert row.error["type"] == "TimeoutError"
    marks = await _marks(steps_pool, run.run_id)
    assert (marks["b:start"], marks["b:done"], marks["c:start"]) == (1, 0, 0)
    assert executor.active_run_ids() == []


async def test_drain_releases_after_current_step(make_executor, repo, saver, steps_pool):
    executor = make_executor()
    steps.FAULTS["drain"] = Fault(delay_s={"b": 1})
    run = await _claimed(repo, "drain")

    task = asyncio.create_task(executor.execute(run))
    await _wait_for_mark(steps_pool, run.run_id, "b:start")
    assert executor.active_run_ids() == [run.run_id]
    assert executor.request_drain_all("test") == 1

    assert await task == RunStatus.PENDING
    row = await _get(repo, run.run_id)
    assert (row.status, row.lease_owner, row.lease_expires_at, row.attempt, row.error) == (
        RunStatus.PENDING,
        None,
        None,
        0,
        None,
    )
    drained = await _marks(steps_pool, run.run_id)
    assert (drained["b:done"], drained["c:start"]) == (1, 0)
    assert executor.active_run_ids() == []
    assert executor.request_drain_all() == 0

    resumed = await repo.claim(run.run_id, OWNER, LEASE_S)
    assert resumed is not None
    assert await executor.execute(resumed) == RunStatus.SUCCESS
    assert await _marks(steps_pool, run.run_id) - drained == Counter({"c:start": 1, "c:done": 1})
    assert await _log(saver, run.run_id) == ["a", "b", "c"]


async def test_configurable_holds_only_thread_id(make_executor, repo, monkeypatch):
    calls = []
    ainvoke = CompiledStateGraph.ainvoke

    async def spy(self, input, config=None, **kwargs):
        calls.append(config)
        return await ainvoke(self, input, config, **kwargs)

    monkeypatch.setattr(CompiledStateGraph, "ainvoke", spy)
    executor = make_executor(metadata=lambda run: {"agent": run.agent})
    run = await _claimed(repo, "p14")

    assert await executor.execute(run) == RunStatus.SUCCESS

    # P14: the run's identity reaches the graph through thread_id only.
    [config] = calls
    assert config["configurable"].keys() == {"thread_id"}
    assert config["configurable"]["thread_id"] == str(run.run_id)
    assert config["metadata"] == {"agent": STEPS_SPEC.name}


@pytest.mark.parametrize("outcome", ["success", "error", "drained"])
async def test_lost_lease_changes_nothing(make_executor, repo, steps_pool, outcome):
    executor = make_executor()
    if outcome == "error":
        steps.FAULTS["lost"] = Fault(fail_step="a", kind="fatal")
    if outcome == "drained":
        steps.FAULTS["lost"] = Fault(delay_s={"a": 0.5})
    # pod-a's lease has already expired, so pod-b takes the run over before pod-a runs it.
    run = await _claimed(repo, "lost", lease_s=-1)
    run_id = run.run_id
    assert await repo.claim(run_id, "pod-b", LEASE_S) is not None

    with structlog.testing.capture_logs() as logs:
        if outcome == "drained":
            task = asyncio.create_task(executor.execute(run))
            await _wait_for_mark(steps_pool, run_id, "a:start")
            executor.request_drain_all()
            status = await task
        else:
            status = await executor.execute(run)

    assert status == RunStatus.RUNNING
    row = await _get(repo, run_id)
    assert (row.status, row.lease_owner, row.attempt, row.error) == (
        RunStatus.RUNNING,
        "pod-b",
        0,
        None,
    )
    [lost] = [entry for entry in logs if entry["event"] == "run.lease_lost"]
    assert lost["log_level"] == "warning"
    assert (lost["run_id"], lost["agent"], lost["attempt"], lost["status"]) == (
        str(run_id),
        STEPS_SPEC.name,
        1,
        RunStatus.RUNNING,
    )
    assert executor.active_run_ids() == []
