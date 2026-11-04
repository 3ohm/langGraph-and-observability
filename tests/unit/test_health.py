"""The health endpoints and the readiness probe (spec 8.6; P5).

No test needs Docker. A pool that points at port 1 on the loopback stands for a
database that is down; a fake pool stands for one that answers.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import httpx
import pytest
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.persistence.pool import create_pool
from langgraph_postgres_starter.settings import load_settings
from langgraph_postgres_starter.worker import health
from langgraph_postgres_starter.worker.health import ReadinessProbe, create_health_app

# Nothing listens on port 1, so every connection attempt is refused at once.
DEAD_DSN = "postgresql://lps:not-a-real-password@127.0.0.1:1/lps"


class _FakeConnection:
    """A connection that answers every query; ``hang`` makes each query wait forever."""

    def __init__(self, *, hang: bool = False) -> None:
        self.hang = hang
        self.queries: list[str] = []

    async def execute(self, query: str) -> None:
        self.queries.append(query)
        if self.hang:
            await asyncio.Event().wait()


class _FakePool:
    """An open pool that lends one fake connection: Postgres is up."""

    closed = False

    def __init__(self, connection: _FakeConnection) -> None:
        self.conn = connection

    # The signature of AsyncConnectionPool.connection, which takes a timeout.
    @asynccontextmanager
    async def connection(
        self,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> AsyncIterator[_FakeConnection]:
        yield self.conn


def _fake_pool(connection: _FakeConnection) -> AsyncConnectionPool[Any]:
    return cast(AsyncConnectionPool[Any], _FakePool(connection))


def _client(probe: ReadinessProbe) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=create_health_app(probe))
    return httpx.AsyncClient(transport=transport, base_url="http://health")


async def test_ok_always_200():
    # The database is down and the intake is dead: liveness asks only that the loop answers.
    probe = ReadinessProbe(create_pool(load_settings(), dsn=DEAD_DSN), intake_alive=lambda: False)

    async with _client(probe) as client:
        response = await client.get("/ok")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_503_when_database_down(monkeypatch):
    # The pool is open, but Postgres does not answer (P5). A short timeout keeps the test fast.
    monkeypatch.setattr(health, "DATABASE_TIMEOUT_S", 0.2)
    pool = create_pool(load_settings(), dsn=DEAD_DSN)
    await pool.open(wait=False)
    try:
        probe = ReadinessProbe(pool, intake_alive=lambda: True)
        async with _client(probe) as client:
            response = await client.get("/ready")
    finally:
        await pool.close()

    assert response.status_code == 503
    body = response.json()
    assert body["checks"]["database"] is False
    assert body == {
        "ready": False,
        "checks": {"database": False, "pool_open": True, "intake": True},
    }


async def test_ready_200_when_all_ok():
    connection = _FakeConnection()
    probe = ReadinessProbe(_fake_pool(connection), intake_alive=lambda: True)

    async with _client(probe) as client:
        response = await client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {
        "ready": True,
        "checks": {"database": True, "pool_open": True, "intake": True},
    }
    assert connection.queries == ["SELECT 1"]


async def test_ready_503_when_pool_not_open():
    # create_pool returns a pool that is not open yet; a closed pool behaves the same.
    probe = ReadinessProbe(create_pool(load_settings(), dsn=DEAD_DSN), intake_alive=lambda: True)

    ready, checks = await probe.check()

    assert ready is False
    assert checks == {"database": False, "pool_open": False, "intake": True}


async def test_database_check_times_out(monkeypatch):
    # The connection is up, but SELECT 1 never returns: the check gives up after the timeout.
    monkeypatch.setattr(health, "DATABASE_TIMEOUT_S", 0.05)
    probe = ReadinessProbe(_fake_pool(_FakeConnection(hang=True)), intake_alive=lambda: True)

    ready, checks = await probe.check()

    assert ready is False
    assert checks == {"database": False, "pool_open": True, "intake": True}


def _raise() -> bool:
    raise RuntimeError("intake task crashed")


@pytest.mark.parametrize("intake_alive", [lambda: False, _raise], ids=["dead", "raises"])
async def test_ready_503_when_intake_not_alive(intake_alive):
    probe = ReadinessProbe(_fake_pool(_FakeConnection()), intake_alive=intake_alive)

    async with _client(probe) as client:
        response = await client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {
        "ready": False,
        "checks": {"database": True, "pool_open": True, "intake": False},
    }


def test_default_database_timeout_is_two_seconds():
    assert health.DATABASE_TIMEOUT_S == 2.0
