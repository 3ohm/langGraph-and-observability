"""The LangGraph Store on the shared pool against a real Postgres (P24)."""

import uuid
from typing import TypedDict

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.graph import START, StateGraph
from langgraph.runtime import Runtime

from langgraph_and_observability.persistence.checkpointer import make_checkpointer
from langgraph_and_observability.persistence.migrate import LANGGRAPH, run_migrate
from langgraph_and_observability.persistence.pool import create_pool, open_pool
from langgraph_and_observability.persistence.serde import make_serde
from langgraph_and_observability.persistence.store import make_store
from langgraph_and_observability.settings import Settings, load_settings

pytestmark = pytest.mark.integration

NAMESPACE = ("memory", "user-1")


class State(TypedDict, total=False):
    count: int


async def count_visits(state: State, runtime: Runtime[None]) -> State:
    """Add 1 to the count in the Store, which outlives the thread, and return it."""
    store = runtime.store
    assert store is not None
    item = await store.aget(NAMESPACE, "count")
    count = (0 if item is None else item.value["count"]) + 1
    await store.aput(NAMESPACE, "count", {"count": count})
    return {"count": count}


async def _counts(settings: Settings) -> list[int]:
    """Migrate, then invoke the graph on two threads; return each invocation's count."""
    await run_migrate(settings, components={LANGGRAPH})
    pool = create_pool(settings)
    await open_pool(pool, timeout=30)
    try:
        builder = StateGraph(State)
        builder.add_node(count_visits)
        builder.add_edge(START, "count_visits")
        saver = make_checkpointer(pool, make_serde([]), pgbouncer_mode=settings.pgbouncer_mode)
        graph = builder.compile(checkpointer=saver, store=make_store(pool))
        counts = []
        for _ in range(2):
            # A new thread starts with no state: the count comes from the Store.
            config: RunnableConfig = {"configurable": {"thread_id": str(uuid.uuid7())}}
            result = await graph.ainvoke({}, config, durability="sync")
            counts.append(result["count"])
        return counts
    finally:
        await pool.close()


async def test_store_keeps_memory_across_threads(fresh_dsn):
    assert await _counts(load_settings(database_url=fresh_dsn)) == [1, 2]


async def test_store_works_in_pgbouncer_mode(fresh_dsn):
    # PgBouncer mode: the pool's connections never prepare a statement.
    settings = load_settings(database_url=fresh_dsn, pgbouncer_mode=True)

    assert await _counts(settings) == [1, 2]
