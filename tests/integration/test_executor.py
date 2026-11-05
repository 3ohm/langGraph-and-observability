"""The run executor against a real Postgres (spec 8.2-8.5; P11, P14)."""

import asyncio
import time
from collections import Counter
from collections.abc import Mapping
from dataclasses import replace
from typing import Any
from uuid import UUID

import pytest
import structlog
from langgraph.graph import START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.runtime import Runtime
from langgraph.store.base import BaseStore
from langgraph.store.memory import InMemoryStore
from langgraph.types import interrupt

from langgraph_and_observability.agent_spec import AgentSpec
from langgraph_and_observability.persistence.checkpointer import make_checkpointer
from langgraph_and_observability.persistence.serde import make_serde
from langgraph_and_observability.policies import default_retry_policy, step_timeout
from langgraph_and_observability.settings import Settings, load_settings
from langgraph_and_observability.worker.executor import RunExecutor
from langgraph_and_observability.worker.runs import RunRepo, RunRow, RunStatus
from tests.graphs import steps
from tests.graphs.steps import STEPS_SPEC, Fault, StepsContext, StepsState

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
    """Build a RunExecutor for one agent, by default the steps agent.

    Keyword arguments other than ``metadata``, ``spec`` and ``store`` override settings.
    """

    def make(
        metadata=None, spec: AgentSpec = STEPS_SPEC, store: BaseStore | None = None, **settings
    ):
        return RunExecutor(
            saver=saver,
            agents={spec.name: spec},
            repo=repo,
            settings=load_settings(**settings),
            owner=OWNER,
            metadata=metadata,
            store=store,
        )

    return make


async def _claimed(repo: RunRepo, key: str, lease_s: float = LEASE_S) -> RunRow:
    """Create a steps run for ``key`` and claim it for OWNER, with a lease of ``lease_s``."""
    return await _claim(repo, await _create(repo, STEPS_SPEC.name, key), lease_s)


async def _create(repo: RunRepo, agent: str, key: str) -> UUID:
    """Create a pending run of ``agent``, which takes the steps input, for ``key``."""
    run_id, created = await repo.create(agent, f"{agent}:{key}", {"key": key})
    assert created
    return run_id


async def _claim(repo: RunRepo, run_id: UUID, lease_s: float = LEASE_S) -> RunRow:
    """Claim the pending run for OWNER, as the recovery loop does."""
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

    # CompiledStateGraph.ainvoke's signature.
    async def spy(self, input, config=None, **kwargs):  # pylint: disable=redefined-builtin
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


async def test_agent_trace_metadata_sits_under_the_core_keys(make_executor, repo, monkeypatch):
    calls = []
    ainvoke = CompiledStateGraph.ainvoke

    # CompiledStateGraph.ainvoke's signature.
    async def spy(self, input, config=None, **kwargs):  # pylint: disable=redefined-builtin
        calls.append(config)
        return await ainvoke(self, input, config, **kwargs)

    def agent_keys(run_input: Mapping[str, Any], settings: Settings) -> dict[str, str]:
        # "agent" is also a core key, which wins.
        return {"key": run_input["key"], "git_sha": settings.git_sha, "agent": "spoofed"}

    monkeypatch.setattr(CompiledStateGraph, "ainvoke", spy)
    spec = replace(STEPS_SPEC, name="test_traced", trace_metadata=agent_keys)
    executor = make_executor(
        metadata=lambda run: {"run_id": str(run.run_id), "agent": run.agent},
        spec=spec,
        git_sha="abc123",
    )
    run_id = await _create(repo, spec.name, "md")

    assert await executor.execute(await _claim(repo, run_id)) == RunStatus.SUCCESS

    [config] = calls
    assert config["metadata"] == {
        "key": "md",
        "git_sha": "abc123",
        "run_id": str(run_id),
        "agent": "test_traced",
    }


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


def _graph(*nodes) -> StateGraph[StepsState, StepsContext, StepsState, StepsState]:
    """A steps-state graph that runs ``nodes`` in order, with the policies of real agents."""
    builder = StateGraph(StepsState, context_schema=StepsContext)
    builder.set_node_defaults(retry_policy=default_retry_policy(), timeout=step_timeout(30))
    previous = START
    for node in nodes:
        builder.add_node(node)
        builder.add_edge(previous, node.__name__)
        previous = node.__name__
    return builder


async def wait_for_approval(state: StepsState) -> dict[str, list[str]]:
    # The value can hold data from the state, so the run's error never names it.
    interrupt("approve ticket OPS-4242")
    return {"log": ["approved"]}


async def test_interrupt_is_a_failed_attempt_with_a_retry(make_executor, repo):
    spec = replace(STEPS_SPEC, name="test_interrupt", build_graph=lambda: _graph(wait_for_approval))
    executor = make_executor(spec=spec, max_attempts=3)
    run_id = await _create(repo, spec.name, "always")

    statuses = []
    for _ in range(3):
        statuses.append(await executor.execute(await _claim(repo, run_id)))
        assert (await _get(repo, run_id)).status != RunStatus.SUCCESS

    # An interrupt is an error like any other: a retry, then the last attempt fails the run.
    assert statuses == [RunStatus.PENDING, RunStatus.PENDING, RunStatus.FAILED]
    row = await _get(repo, run_id)
    assert (row.status, row.attempt) == (RunStatus.FAILED, 3)
    assert row.error is not None
    assert row.error["type"] == "InterruptNotSupportedError"
    assert "langgraph dev" in row.error["message"]
    assert "wait_for_approval" in row.error["message"]
    assert "OPS-4242" not in row.error["message"]


async def test_retry_after_an_interrupt_completes_when_the_condition_clears(make_executor, repo):
    calls: Counter[str] = Counter()
    approved = False

    async def first(state: StepsState) -> dict[str, list[str]]:
        calls["first"] += 1
        return {"log": ["first"]}

    async def gate(state: StepsState) -> dict[str, list[str]]:
        calls["gate"] += 1
        if not approved:
            interrupt("not approved yet")
        return {"log": ["gate"]}

    spec = replace(STEPS_SPEC, name="test_gate", build_graph=lambda: _graph(first, gate))
    executor = make_executor(spec=spec)
    run_id = await _create(repo, spec.name, "gate")

    assert await executor.execute(await _claim(repo, run_id)) == RunStatus.PENDING
    approved = True
    assert await executor.execute(await _claim(repo, run_id)) == RunStatus.SUCCESS

    # The retry runs only the interrupted node again; first completed and does not run again.
    assert calls == Counter({"first": 1, "gate": 2})
    row = await _get(repo, run_id)
    assert (row.status, row.attempt) == (RunStatus.SUCCESS, 1)


async def remember(state: StepsState, runtime: Runtime[StepsContext]) -> dict[str, list[str]]:
    """Put an item in the Store, under the run's key."""
    assert runtime.store is not None
    await runtime.store.aput(
        ("memory", state["key"]), "last_run", {"run_id": str(runtime.context.run.run_id)}
    )
    return {"log": ["remembered"]}


async def test_executor_compiles_graphs_with_the_store(make_executor, repo):
    store = InMemoryStore()
    spec = replace(STEPS_SPEC, name="test_store", build_graph=lambda: _graph(remember))
    executor = make_executor(spec=spec, store=store)
    run_id = await _create(repo, spec.name, "user-1")

    assert await executor.execute(await _claim(repo, run_id)) == RunStatus.SUCCESS

    item = await store.aget(("memory", "user-1"), "last_run")
    assert item is not None
    assert item.value == {"run_id": str(run_id)}
