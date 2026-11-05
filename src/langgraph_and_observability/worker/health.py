"""Liveness and readiness endpoints for the worker (spec §8.6; P5).

The worker serves ``create_health_app`` on ``APP_HEALTH_PORT``:

- ``GET /ok`` is liveness. It answers 200 whenever the event loop runs.
- ``GET /ready`` is readiness. It answers 200 when every check of ``ReadinessProbe``
  passes, else 503. Each response names the result of each check.

A check that fails returns False; the probe never raises. It logs nothing, so it never
writes the DSN, which holds the database password.
"""

import asyncio
from collections.abc import Callable

from fastapi import FastAPI, Response
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]

# The time that Postgres has to answer SELECT 1, including the wait for a connection.
DATABASE_TIMEOUT_S = 2.0


class ReadinessProbe:
    """Decides whether the worker is ready: whether it can do its work right now.

    The checks are:

    - ``database``: Postgres answers ``SELECT 1`` through ``pool`` within
      ``DATABASE_TIMEOUT_S`` seconds;
    - ``pool_open``: ``pool`` is open, not closed or never opened;
    - ``intake``: ``intake_alive()`` returns True. An exception counts as False.
    """

    def __init__(self, pool: _Pool, *, intake_alive: Callable[[], bool]) -> None:
        self._pool = pool
        self._intake_alive = intake_alive

    async def check(self) -> tuple[bool, dict[str, bool]]:
        """Run every check. Returns whether all passed, and each check's result by name."""
        checks = {
            "database": await self._database(),
            "pool_open": not self._pool.closed,
            "intake": self._intake(),
        }
        return all(checks.values()), checks

    async def _database(self) -> bool:
        try:
            async with asyncio.timeout(DATABASE_TIMEOUT_S):
                async with self._pool.connection(timeout=DATABASE_TIMEOUT_S) as conn:
                    await conn.execute("SELECT 1")
        except Exception:  # pylint: disable=broad-exception-caught  # any failure: not ready
            return False
        return True

    def _intake(self) -> bool:
        try:
            return bool(self._intake_alive())
        except Exception:  # pylint: disable=broad-exception-caught  # any failure: not ready
            return False


def create_health_app(probe: ReadinessProbe) -> FastAPI:
    """Return the FastAPI app that serves ``/ok`` and ``/ready`` from ``probe``.

    The app has no OpenAPI schema or docs pages: it serves the two probes only.
    """
    app = FastAPI(
        title="langgraph-and-observability health",
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )

    @app.get("/ok")
    async def ok() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready(response: Response) -> dict[str, object]:
        is_ready, checks = await probe.check()
        response.status_code = 200 if is_ready else 503
        return {"ready": is_ready, "checks": checks}

    return app
