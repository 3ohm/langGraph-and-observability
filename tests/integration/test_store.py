"""The LangGraph Store on the shared pool against a real Postgres (P24)."""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TypedDict

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.graph import START, StateGraph
from langgraph.runtime import Runtime
from langgraph.store.base import PutOp
from langgraph.store.postgres.aio import AsyncPostgresStore
from psycopg import AsyncConnection, pq
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from langgraph_and_observability.persistence.checkpointer import make_checkpointer
from langgraph_and_observability.persistence.migrate import LANGGRAPH, run_migrate
from langgraph_and_observability.persistence.pool import create_pool, open_pool
from langgraph_and_observability.persistence.serde import make_serde
from langgraph_and_observability.persistence.store import make_store
from langgraph_and_observability.settings import Settings, load_settings

pytestmark = pytest.mark.integration

NAMESPACE = ("memory", "user-1")

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]


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


@asynccontextmanager
async def _migrated_pool(settings: Settings) -> AsyncIterator[_Pool]:
    """Run the ``langgraph`` migrations, then lend an open pool from ``create_pool``."""
    await run_migrate(settings, components={LANGGRAPH})
    pool = create_pool(settings)
    await open_pool(pool, timeout=30)
    try:
        yield pool
    finally:
        await pool.close()


async def _counts(pool: _Pool, settings: Settings) -> list[int]:
    """Invoke the graph on two threads; return each invocation's count."""
    builder = StateGraph(State)
    builder.add_node(count_visits)
    builder.add_edge(START, "count_visits")
    mode = settings.pgbouncer_mode
    saver = make_checkpointer(pool, make_serde([]), pgbouncer_mode=mode)
    graph = builder.compile(checkpointer=saver, store=make_store(pool, pgbouncer_mode=mode))
    counts = []
    for _ in range(2):
        # A new thread starts with no state: the count comes from the Store.
        config: RunnableConfig = {"configurable": {"thread_id": str(uuid.uuid7())}}
        result = await graph.ainvoke({}, config, durability="sync")
        counts.append(result["count"])
    return counts


async def test_store_keeps_memory_across_threads(fresh_dsn):
    settings = load_settings(database_url=fresh_dsn)
    async with _migrated_pool(settings) as pool:
        assert await _counts(pool, settings) == [1, 2]


async def test_store_works_in_pgbouncer_mode(fresh_dsn, monkeypatch):
    # PgBouncer mode: the pool's connections never prepare a statement, and the Store's
    # batches run in a plain transaction, not in pipeline mode.
    settings = load_settings(database_url=fresh_dsn, pgbouncer_mode=True)
    modes = []
    cursor = AsyncPostgresStore._cursor

    @asynccontextmanager
    async def spy(self, *, pipeline=False):
        async with cursor(self, pipeline=pipeline) as cur:
            conn = cur.connection
            modes.append((pipeline, conn.pgconn.pipeline_status, conn.info.transaction_status))
            yield cur

    async with _migrated_pool(settings) as pool:
        # After run_migrate, whose setup() also uses _cursor (without a pipeline).
        monkeypatch.setattr(AsyncPostgresStore, "_cursor", spy)
        store = make_store(pool, pgbouncer_mode=True)
        assert store.supports_pipeline is False
        # One batch of two writes, as a node's writes in the same tick are batched.
        await store.abatch([PutOp(NAMESPACE, "a", {"n": 1}), PutOp(NAMESPACE, "b", {"n": 2})])
        assert modes == [(True, pq.PipelineStatus.OFF, pq.TransactionStatus.INTRANS)]
        items = await store.asearch(NAMESPACE)
        assert {item.key: item.value for item in items} == {"a": {"n": 1}, "b": {"n": 2}}

        assert await _counts(pool, settings) == [1, 2]
