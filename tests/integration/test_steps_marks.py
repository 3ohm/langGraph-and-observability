"""The steps graph records its marks in test_step_calls when a pool is registered."""

from datetime import UTC, datetime, timedelta

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from langgraph_and_observability.agent_spec import RunInfo
from langgraph_and_observability.ids import new_id
from langgraph_and_observability.persistence.resources import set_pool
from langgraph_and_observability.persistence.serde import make_serde
from langgraph_and_observability.settings import load_settings
from tests.graphs import steps
from tests.graphs.steps import STEPS_SPEC, Fault

pytestmark = pytest.mark.integration


@pytest.fixture
async def marks_pool(pool):
    """The test pool with test_step_calls, registered for the steps to find."""
    await steps.ensure_table(pool)
    await steps.ensure_table(pool)  # a second call is a no-op
    set_pool(pool)
    yield pool
    set_pool(None)
    steps.FAULTS.clear()


async def test_marks_go_to_test_step_calls(marks_pool):
    steps.FAULTS["flaky"] = Fault(fail_step="b", kind="transient", times=1, delay_s={"b": 0.2})
    model = STEPS_SPEC.input_model.model_validate({"key": "flaky"})
    run = RunInfo(new_id(), STEPS_SPEC.dedup_key(model, datetime.now(UTC)), attempt=1)
    graph = STEPS_SPEC.build_graph().compile(
        checkpointer=InMemorySaver(serde=make_serde(STEPS_SPEC.serde_types))
    )

    result = await graph.ainvoke(
        STEPS_SPEC.initial_state(model),
        {"configurable": {"thread_id": str(run.run_id)}},
        context=STEPS_SPEC.make_context(run, model, load_settings()),
        durability="sync",
    )

    assert result["log"] == ["a", "b", "c"]
    async with marks_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT mark, at FROM test_step_calls WHERE run_id = %s ORDER BY at",
            (str(run.run_id),),
        )
        rows = await cur.fetchall()
    marks = [row["mark"] for row in rows]
    assert marks == ["a:start", "a:done", "b:start", "b:start", "b:done", "c:start", "c:done"]
    # The delay runs between the step's start and done marks.
    assert rows[4]["at"] - rows[3]["at"] >= timedelta(seconds=0.2)
    assert run.run_id not in steps.MEMORY_MARKS
