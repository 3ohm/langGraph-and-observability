"""The LangGraph checkpointer on the shared connection pool (spec §7.2).

The stock ``AsyncPostgresSaver`` holds one lock per saver around every database call,
even when it runs on a pool, so a process makes one checkpoint call at a time.
``PoolAwarePostgresSaver`` takes no shared lock on a pool, as ``AsyncPostgresStore``
does: each call checks out its own connection.

The checkpoint tables come from ``run_migrate`` (the ``langgraph`` component), not
from this module.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from langgraph.checkpoint.postgres import _ainternal
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.serde.base import SerializerProtocol
from psycopg import AsyncConnection, AsyncCursor
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool


class PoolAwarePostgresSaver(AsyncPostgresSaver):
    """An ``AsyncPostgresSaver`` that, on a pool, runs checkpoint calls concurrently.

    Delete this class when upstream ``AsyncPostgresSaver._cursor`` stops taking
    ``self.lock`` on a pool. Spec §7.2 asks for an upstream issue and pull request; none
    is open yet: opening them is an owner action (O3 in ``docs/release-checklist.md``).
    Two tests in ``tests/unit/test_upstream_shape.py`` watch the upstream method:
    ``test_upstream_still_takes_instance_lock`` fails when upstream fixes the lock, and
    ``test_upstream_cursor_signature_unchanged`` fails when its signature changes.
    """

    @asynccontextmanager
    async def _cursor(self, *, pipeline: bool = False) -> AsyncIterator[AsyncCursor[DictRow]]:
        # The body of AsyncPostgresSaver._cursor (langgraph-checkpoint-postgres 3.2.0)
        # with one change, as in AsyncPostgresStore._cursor: on a pool each call checks
        # out its own connection, so it takes a fresh lock. A single connection keeps
        # the instance lock.
        lock = asyncio.Lock() if isinstance(self.conn, AsyncConnectionPool) else self.lock
        async with lock, _ainternal.get_connection(self.conn) as conn:
            if self.pipe:
                # a connection in pipeline mode can be used concurrently
                # in multiple threads/coroutines, but only one cursor can be
                # used at a time
                try:
                    async with conn.cursor(binary=True, row_factory=dict_row) as cur:
                        yield cur
                finally:
                    if pipeline:
                        await self.pipe.sync()
            elif pipeline:
                # a connection not in pipeline mode can only be used by one
                # thread/coroutine at a time, so we acquire a lock
                if self.supports_pipeline:
                    async with (
                        conn.pipeline(),
                        conn.cursor(binary=True, row_factory=dict_row) as cur,
                    ):
                        yield cur
                else:
                    # Use connection's transaction context manager when pipeline mode not supported
                    async with (
                        conn.transaction(),
                        conn.cursor(binary=True, row_factory=dict_row) as cur,
                    ):
                        yield cur
            else:
                async with conn.cursor(binary=True, row_factory=dict_row) as cur:
                    yield cur


def make_checkpointer(
    pool: AsyncConnectionPool[AsyncConnection[DictRow]],
    serde: SerializerProtocol,
    *,
    pgbouncer_mode: bool = False,
) -> PoolAwarePostgresSaver:
    """Return a ``PoolAwarePostgresSaver`` on ``pool`` that stores checkpoints with ``serde``.

    Call it from the running event loop: the saver keeps that loop. ``pgbouncer_mode``
    (``APP_PGBOUNCER_MODE``) turns off the saver's pipeline mode, so its writes run in a
    plain transaction (spec §7.1, upstream issue #8420).
    """
    saver = PoolAwarePostgresSaver(pool, serde=serde)
    if pgbouncer_mode:
        # _cursor(pipeline=True), which aput, aput_writes and adelete_thread use, reads it.
        saver.supports_pipeline = False
    return saver
