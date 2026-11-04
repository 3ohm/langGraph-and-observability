"""The pool-aware checkpointer against a real Postgres (spec 7.1, 7.2; P3 and P7)."""

import asyncio
import dataclasses
import operator
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Annotated, Any, TypedDict

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from psycopg import AsyncConnection, pq
from psycopg.rows import DictRow

from langgraph_postgres_starter.persistence.checkpointer import (
    PoolAwarePostgresSaver,
    make_checkpointer,
)
from langgraph_postgres_starter.persistence.migrate import LANGGRAPH, run_migrate
from langgraph_postgres_starter.persistence.serde import make_serde
from langgraph_postgres_starter.settings import load_settings

pytestmark = pytest.mark.integration


@pytest.fixture
async def checkpoint_tables(fresh_dsn):
    """Create LangGraph's checkpoint tables in the test database."""
    await run_migrate(load_settings(database_url=fresh_dsn), components={LANGGRAPH})


class _Holders:
    """Wraps pool.connection(): counts the connections held at once, and holds each 50 ms."""

    def __init__(
        self, connection: Callable[..., AbstractAsyncContextManager[AsyncConnection[DictRow]]]
    ) -> None:
        self._connection = connection
        self.now = 0
        self.peak = 0

    @asynccontextmanager
    async def connection(
        self, *args: Any, **kwargs: Any
    ) -> AsyncIterator[AsyncConnection[DictRow]]:
        async with self._connection(*args, **kwargs) as conn:
            self.now += 1
            self.peak = max(self.peak, self.now)
            try:
                await asyncio.sleep(0.05)
                yield conn
            finally:
                self.now -= 1


async def _peak_holders(holders: _Holders, saver: AsyncPostgresSaver) -> int:
    """Run 8 aget_tuple calls at once; return the most connections they held at once."""
    holders.peak = 0
    async with asyncio.TaskGroup() as tg:
        for _ in range(8):
            config: RunnableConfig = {"configurable": {"thread_id": str(uuid.uuid7())}}
            tg.create_task(saver.aget_tuple(config))
    return holders.peak


async def test_pool_aware_saver_is_concurrent(pool, checkpoint_tables, monkeypatch):
    holders = _Holders(pool.connection)
    monkeypatch.setattr(pool, "connection", holders.connection)
    serde = make_serde([])

    pool_aware = await _peak_holders(holders, make_checkpointer(pool, serde))
    stock = await _peak_holders(holders, AsyncPostgresSaver(pool, serde=serde))

    assert pool_aware > 1
    assert stock == 1


async def test_pgbouncer_mode_disables_pipeline(pool):
    default = make_checkpointer(pool, make_serde([]))
    pgbouncer = make_checkpointer(pool, make_serde([]), pgbouncer_mode=True)

    assert pgbouncer.supports_pipeline is False
    # aput and aput_writes use _cursor(pipeline=True): a pipeline by default, and a plain
    # transaction in PgBouncer mode.
    async with default._cursor(pipeline=True) as cur:
        assert cur.connection.pgconn.pipeline_status == pq.PipelineStatus.ON
    async with pgbouncer._cursor(pipeline=True) as cur:
        assert cur.connection.pgconn.pipeline_status == pq.PipelineStatus.OFF
        assert cur.connection.info.transaction_status == pq.TransactionStatus.INTRANS


@dataclasses.dataclass
class Finding:
    title: str
    score: int


class State(TypedDict, total=False):
    steps: Annotated[list[str], operator.add]
    finding: Finding


async def _first(state: State) -> State:
    return {"steps": ["first"]}


async def _second(state: State) -> State:
    return {"steps": ["second"], "finding": Finding(title="disk full", score=3)}


@pytest.mark.usefixtures("strict_msgpack")
@pytest.mark.parametrize("pgbouncer_mode", [False, True], ids=["pipeline", "pgbouncer"])
async def test_graph_round_trip_with_pool_aware_saver(pool, checkpoint_tables, pgbouncer_mode):
    builder = StateGraph(State)
    builder.add_node("first", _first)
    builder.add_node("second", _second)
    builder.add_edge(START, "first")
    builder.add_edge("first", "second")
    builder.add_edge("second", END)

    def compile_graph() -> CompiledStateGraph[State, None, State, State]:
        saver = make_checkpointer(pool, make_serde([Finding]), pgbouncer_mode=pgbouncer_mode)
        return builder.compile(checkpointer=saver)

    graph = compile_graph()
    # In strict mode, compile() swaps in a copy of the saver; the copy keeps its settings.
    assert isinstance(graph.checkpointer, PoolAwarePostgresSaver)
    assert graph.checkpointer.supports_pipeline is not pgbouncer_mode
    config: RunnableConfig = {"configurable": {"thread_id": str(uuid.uuid7())}}
    await graph.ainvoke({"steps": []}, config, durability="sync")

    # A new saver and graph: the state comes from Postgres, not from memory.
    snapshot = await compile_graph().aget_state(config)

    assert snapshot.values == {
        "steps": ["first", "second"],
        "finding": Finding(title="disk full", score=3),
    }
    assert snapshot.next == ()
