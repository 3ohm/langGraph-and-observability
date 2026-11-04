"""The action ledger against a real Postgres (spec 7.3, 9.9; P13)."""

import asyncio
from typing import Any

import pytest
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.persistence.ledger import LedgerConflict, run_once
from langgraph_postgres_starter.persistence.migrate import CORE_MIGRATIONS_DIR, apply, discover

pytestmark = pytest.mark.integration

KEY = "publish:r-1:log"


@pytest.fixture
async def ledger(pool):
    """The test pool, with the core migrations (the action_ledger table) applied."""
    await apply(pool, discover([("core", CORE_MIGRATIONS_DIR)]))
    return pool


class _Action:
    """A fake side effect: counts its calls, sleeps ``delay`` s, returns a message id."""

    def __init__(self, delay: float = 0.0) -> None:
        self.delay = delay
        self.calls = 0

    async def __call__(self) -> dict[str, Any]:
        self.calls += 1
        await asyncio.sleep(self.delay)
        return {"message_id": f"m-{self.calls}"}


async def _rows(pool: AsyncConnectionPool[AsyncConnection[DictRow]]) -> list[DictRow]:
    """Return every action_ledger row's key, action and result."""
    async with pool.connection() as conn:
        cur = await conn.execute("select key, action, result from action_ledger")
        return await cur.fetchall()


async def test_second_call_replays(ledger):
    fn = _Action()

    out1 = await run_once(
        ledger, key=KEY, action="publish", args={"run_id": "r-1", "sink": "log"}, fn=fn
    )
    # The same arguments in another order: the hash is of canonical JSON.
    out2 = await run_once(
        ledger, key=KEY, action="publish", args={"sink": "log", "run_id": "r-1"}, fn=fn
    )

    assert fn.calls == 1
    assert out1.replayed is False
    assert out2.replayed is True
    assert out2.result == out1.result == {"message_id": "m-1"}
    assert await _rows(ledger) == [
        {"key": KEY, "action": "publish", "result": {"message_id": "m-1"}}
    ]


async def test_concurrent_same_key_runs_once(ledger):
    fn = _Action(delay=0.2)

    outs = await asyncio.gather(
        *(
            run_once(ledger, key=KEY, action="publish", args={"run_id": "r-1"}, fn=fn)
            for _ in range(5)
        )
    )

    assert fn.calls == 1
    assert sorted(out.replayed for out in outs) == [False, True, True, True, True]
    assert all(out.result == {"message_id": "m-1"} for out in outs)


async def test_failed_fn_records_nothing(ledger):
    error = RuntimeError("sink down")

    async def failing() -> dict[str, Any]:
        raise error

    with pytest.raises(RuntimeError) as raised:
        await run_once(ledger, key=KEY, action="publish", args={"run_id": "r-1"}, fn=failing)

    assert raised.value is error
    assert await _rows(ledger) == []
    # The key is still free: the next call runs its action.
    fn = _Action()
    out = await run_once(ledger, key=KEY, action="publish", args={"run_id": "r-1"}, fn=fn)
    assert (out.replayed, fn.calls) == (False, 1)


async def test_args_mismatch_raises(ledger):
    fn = _Action()
    await run_once(ledger, key=KEY, action="publish", args={"run_id": "r-1"}, fn=fn)

    with pytest.raises(LedgerConflict):
        await run_once(ledger, key=KEY, action="publish", args={"run_id": "r-2"}, fn=fn)

    assert fn.calls == 1
    assert await _rows(ledger) == [
        {"key": KEY, "action": "publish", "result": {"message_id": "m-1"}}
    ]
