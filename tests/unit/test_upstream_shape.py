"""The upstream shapes that the package depends on.

PoolAwarePostgresSaver copies the body of AsyncPostgresSaver._cursor and changes only
the lock (spec 7.2; P3). Two tests fail when that private method changes shape, so the
copy is reviewed again, or deleted once upstream fixes the lock.

The run executor finds an interrupt by a key in the result of ``ainvoke``
(``worker/executor.py``). A test here fails when LangGraph stops returning that key.
"""

import inspect
from typing import TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import START, StateGraph
from langgraph.types import interrupt


def test_upstream_cursor_signature_unchanged():
    assert list(inspect.signature(AsyncPostgresSaver._cursor).parameters) == ["self", "pipeline"]


def test_upstream_still_takes_instance_lock():
    # When this fails, upstream fixed the lock:
    # delete PoolAwarePostgresSaver (docs/practices/P3.md).
    assert "async with self.lock" in inspect.getsource(AsyncPostgresSaver._cursor)


class _AskState(TypedDict):
    answer: str


async def ask(state: _AskState) -> dict[str, str]:
    return {"answer": interrupt("need input")}


async def test_ainvoke_result_names_an_interrupt():
    # LangGraph 1.x made its INTERRUPT constant private; the documented key is the literal.
    builder = StateGraph(_AskState)
    builder.add_node(ask)
    builder.add_edge(START, "ask")
    graph = builder.compile(checkpointer=InMemorySaver())

    result = await graph.ainvoke(
        {"answer": ""}, {"configurable": {"thread_id": "shape"}}, durability="sync"
    )

    assert isinstance(result, dict)
    assert "__interrupt__" in result
