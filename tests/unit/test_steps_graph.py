"""The test-only steps graph (tests/graphs/steps.py) with an in-memory checkpointer.

No pool is registered, so the steps record their marks in steps.MEMORY_MARKS.
"""

import time
from datetime import UTC, datetime
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph.state import CompiledStateGraph

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.persistence.serde import make_serde
from langgraph_postgres_starter.settings import load_settings
from tests.graphs import steps
from tests.graphs.steps import STEPS_SPEC, Fault

ALL_MARKS = ["a:start", "a:done", "b:start", "b:done", "c:start", "c:done"]


@pytest.fixture(autouse=True)
def _reset_steps():
    yield
    steps.FAULTS.clear()
    steps.MEMORY_MARKS.clear()


def _compile() -> CompiledStateGraph[Any, Any, Any, Any]:
    serde = make_serde(STEPS_SPEC.serde_types)
    return STEPS_SPEC.build_graph().compile(checkpointer=InMemorySaver(serde=serde))


def _new_run(key: str) -> RunInfo:
    model = STEPS_SPEC.input_model.model_validate({"key": key})
    return RunInfo(
        run_id=new_id(), dedup_key=STEPS_SPEC.dedup_key(model, datetime.now(UTC)), attempt=1
    )


async def _invoke(graph, run: RunInfo, key: str, *, resume: bool = False) -> dict[str, Any]:
    """Start the run, or with ``resume`` continue it from its last checkpoint."""
    model = STEPS_SPEC.input_model.model_validate({"key": key})
    result: dict[str, Any] = await graph.ainvoke(
        None if resume else STEPS_SPEC.initial_state(model),
        {"configurable": {"thread_id": str(run.run_id)}},
        context=STEPS_SPEC.make_context(run, model, load_settings()),
        durability="sync",
    )
    return result


async def test_steps_graph_runs():
    run = _new_run("ok")

    result = await _invoke(_compile(), run, "ok")

    assert result["log"] == ["a", "b", "c"]
    assert steps.MEMORY_MARKS[run.run_id] == ALL_MARKS


async def test_transient_failure_is_retried():
    # The retry policy waits at least its 0.5 s initial interval before the retry.
    steps.FAULTS["flaky"] = Fault(fail_step="b", kind="transient", times=1)
    run = _new_run("flaky")

    result = await _invoke(_compile(), run, "flaky")

    assert result["log"] == ["a", "b", "c"]
    assert steps.MEMORY_MARKS[run.run_id].count("b:start") == 2
    assert steps.MEMORY_MARKS[run.run_id] == [
        "a:start",
        "a:done",
        "b:start",
        "b:start",
        "b:done",
        "c:start",
        "c:done",
    ]


async def test_fatal_failure_is_not_retried():
    steps.FAULTS["broken"] = Fault(fail_step="b", kind="fatal", times=1)
    run = _new_run("broken")

    with pytest.raises(ValueError, match="fatal fault in step b"):
        await _invoke(_compile(), run, "broken")

    assert steps.MEMORY_MARKS[run.run_id] == ["a:start", "a:done", "b:start"]


async def test_times_counts_every_attempt_of_the_step_in_the_run():
    # The second invocation resumes the same run from its checkpoint, as a run retry
    # does. Its attempt at b is the step's second, so the one-time fault has passed.
    steps.FAULTS["once"] = Fault(fail_step="b", kind="fatal", times=1)
    run = _new_run("once")
    graph = _compile()
    with pytest.raises(ValueError):
        await _invoke(graph, run, "once")

    result = await _invoke(graph, run, "once", resume=True)

    assert result["log"] == ["a", "b", "c"]
    assert steps.MEMORY_MARKS[run.run_id] == ["a:start", "a:done", "b:start", *ALL_MARKS[2:]]


async def test_delay_runs_inside_the_step():
    steps.FAULTS["slow"] = Fault(delay_s={"b": 0.2})
    run = _new_run("slow")

    started = time.monotonic()
    result = await _invoke(_compile(), run, "slow")

    assert time.monotonic() - started >= 0.2
    assert result["log"] == ["a", "b", "c"]
    assert steps.MEMORY_MARKS[run.run_id] == ALL_MARKS


def test_dedup_key_depends_on_the_key_only():
    model = STEPS_SPEC.input_model.model_validate({"key": "k1"})

    first = STEPS_SPEC.dedup_key(model, datetime(2026, 11, 2, tzinfo=UTC))
    later = STEPS_SPEC.dedup_key(model, datetime(2026, 11, 3, tzinfo=UTC))

    assert first == later == "test_steps:k1"
