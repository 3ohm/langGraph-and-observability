"""A three-step test-only graph, ``test_steps``, with injected faults and delays (spec §11).

The graph runs ``step_a``, ``step_b`` and ``step_c`` in order. Each step appends its
name (``a``, ``b`` or ``c``) to ``log`` and records two marks: ``"<step>:start"`` when
it begins and ``"<step>:done"`` when it ends. With a pool registered
(``persistence.resources.set_pool``), the marks go to the ``test_step_calls`` table
(see ``ensure_table``), so tests can see them across processes. Without one, they go to
``MEMORY_MARKS``, by run ID. Tests read and clear ``MEMORY_MARKS`` directly.

Tests put a ``Fault`` in ``FAULTS`` under a run's input ``key`` to make a step slow, or
to make it fail a number of times.
"""

import asyncio
import operator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Literal, TypedDict
from uuid import UUID

from langgraph.graph import START, StateGraph
from langgraph.runtime import Runtime
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from langgraph_postgres_starter.agent_spec import AgentSpec, RunInfo
from langgraph_postgres_starter.persistence.resources import current_pool
from langgraph_postgres_starter.policies import default_retry_policy, step_timeout
from langgraph_postgres_starter.settings import Settings

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]


class StepsInput(BaseModel):
    """The queue message body for a ``test_steps`` run."""

    key: str


class StepsState(TypedDict):
    key: str
    log: Annotated[list[str], operator.add]


@dataclass(frozen=True)
class StepsContext:
    """The run's fixed inputs (the graph's ``context_schema``)."""

    run: RunInfo


@dataclass(frozen=True)
class Fault:
    """What to do to the steps of a run.

    - ``fail_step``: the step that fails (``a``, ``b`` or ``c``), or None.
    - ``kind``: ``transient`` raises ConnectionError, which the retry policy retries;
      ``fatal`` raises ValueError, which it does not.
    - ``times``: how many attempts of ``fail_step`` fail before it succeeds. Every
      attempt in the run counts, also the attempts of a later invocation of the same
      run: the step counts its own ``"<step>:start"`` marks.
    - ``delay_s``: seconds that a step sleeps, by step, between its two marks.
    """

    fail_step: str | None = None
    kind: Literal["transient", "fatal"] = "transient"
    times: int = 1
    delay_s: dict[str, float] = field(default_factory=dict)


FAULTS: dict[str, Fault] = {}
MEMORY_MARKS: dict[UUID, list[str]] = {}


async def ensure_table(pool: _Pool) -> None:
    """Create the ``test_step_calls`` table if it does not exist."""
    async with pool.connection() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS test_step_calls ("
            " run_id text NOT NULL,"
            " mark text NOT NULL,"
            " at timestamptz NOT NULL DEFAULT clock_timestamp())"
        )


async def _record(run_id: UUID, mark: str) -> int:
    """Record ``mark`` for the run. Return how many times the run has recorded it."""
    pool = current_pool()
    if pool is None:
        marks = MEMORY_MARKS.setdefault(run_id, [])
        marks.append(mark)
        return marks.count(mark)
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO test_step_calls (run_id, mark) VALUES (%s, %s)", (str(run_id), mark)
        )
        cur = await conn.execute(
            "SELECT count(*) AS n FROM test_step_calls WHERE run_id = %s AND mark = %s",
            (str(run_id), mark),
        )
        row = await cur.fetchone()
    assert row is not None  # count(*) always returns one row
    count: int = row["n"]
    return count


async def _step(
    step: str, state: StepsState, runtime: Runtime[StepsContext]
) -> dict[str, list[str]]:
    run_id = runtime.context.run.run_id
    attempt = await _record(run_id, f"{step}:start")
    fault = FAULTS.get(state["key"])
    if fault is not None:
        if delay := fault.delay_s.get(step):
            await asyncio.sleep(delay)
        if fault.fail_step == step and attempt <= fault.times:
            error = ConnectionError if fault.kind == "transient" else ValueError
            raise error(f"injected {fault.kind} fault in step {step} (attempt {attempt})")
    await _record(run_id, f"{step}:done")
    return {"log": [step]}


async def step_a(state: StepsState, runtime: Runtime[StepsContext]) -> dict[str, list[str]]:
    return await _step("a", state, runtime)


async def step_b(state: StepsState, runtime: Runtime[StepsContext]) -> dict[str, list[str]]:
    return await _step("b", state, runtime)


async def step_c(state: StepsState, runtime: Runtime[StepsContext]) -> dict[str, list[str]]:
    return await _step("c", state, runtime)


def build_graph() -> StateGraph[StepsState, StepsContext, StepsState, StepsState]:
    builder = StateGraph(StepsState, context_schema=StepsContext)
    builder.set_node_defaults(retry_policy=default_retry_policy(), timeout=step_timeout(30))
    builder.add_node(step_a)
    builder.add_node(step_b)
    builder.add_node(step_c)
    builder.add_edge(START, "step_a")
    builder.add_edge("step_a", "step_b")
    builder.add_edge("step_b", "step_c")
    return builder


def _steps_input(model: BaseModel) -> StepsInput:
    if not isinstance(model, StepsInput):
        raise TypeError(f"expected StepsInput, got {type(model).__name__}")
    return model


def dedup_key(model: BaseModel, sent_at: datetime) -> str:
    """One run per key: the send time does not matter."""
    return f"test_steps:{_steps_input(model).key}"


def make_context(run: RunInfo, model: BaseModel, settings: Settings) -> StepsContext:
    return StepsContext(run=run)


def initial_state(model: BaseModel) -> dict[str, str]:
    return {"key": _steps_input(model).key}


STEPS_SPEC = AgentSpec(
    name="test_steps",
    build_graph=build_graph,
    input_model=StepsInput,
    dedup_key=dedup_key,
    make_context=make_context,
    initial_state=initial_state,
    # The state holds only a str and a list of str, which need no allowlist entry.
    serde_types=(),
)
