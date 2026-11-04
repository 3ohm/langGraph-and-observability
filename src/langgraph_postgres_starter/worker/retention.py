"""Retention: prune old checkpoints and delete old results, on one pod (spec §7.4; P6).

``prune_once`` makes one pass. It first takes the leader lock, the session advisory
lock ``hashtext('lps:prune')``, on a pool connection that it holds for the whole pass.
When another session holds the lock, it returns at once and changes nothing. The
leader then:

1. Prunes checkpoints. A run qualifies when it is ``success`` or ``failed``, ended
   before ``now - retention_checkpoint_days`` and has no ``checkpoints_pruned_at``. For
   each batch of up to ``BATCH_SIZE`` such runs it calls
   ``saver.aprune(thread_ids, strategy="delete")``, which deletes every checkpoint,
   blob and write of their threads (``thread_id == str(run_id)``), and then sets
   ``checkpoints_pruned_at``. It goes on until no run qualifies.
2. Deletes results that ended before ``now - retention_result_days``: it runs each
   agent's ``retention_sql`` with ``cutoff``, then deletes the ``runs`` rows. A run's
   row goes only after its checkpoints have been pruned, so no row disappears while its
   checkpoints remain, even when result retention is shorter than checkpoint retention.

Only finished runs (``success``, ``failed``) are pruned or deleted. A batch keeps its
runs' rows locked (``FOR UPDATE``) until their checkpoints are gone, so a ``retry``
cannot return one of them to pending, and no worker can start it, during the prune.

LangGraph's tables change only through the saver, never through SQL in this module.
Each batch and each statement is retried up to ``MAX_RETRIES`` times on
SerializationFailure and DeadlockDetected, after a short back-off; other errors
propagate. The leader lock is released before its connection goes back to the pool.

The leader lock belongs to a database session, so, as for migrations, the pool must
reach Postgres directly, not through PgBouncer in transaction mode.
"""

import asyncio
import functools
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver
from psycopg import AsyncConnection
from psycopg import errors as pg_errors
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.agent_spec import AgentSpec
from langgraph_postgres_starter.logging import get_logger
from langgraph_postgres_starter.settings import Settings

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]
type _Conn = AsyncConnection[DictRow]

_log = get_logger(__name__)

# The most runs whose checkpoints one aprune call deletes.
BATCH_SIZE = 100
# A batch or statement that fails with one of _RETRYABLE runs again up to this often.
MAX_RETRIES = 3
# The wait before retry n is n times this.
_RETRY_BACKOFF_S = 0.1
_RETRYABLE = (pg_errors.SerializationFailure, pg_errors.DeadlockDetected)

_LOCK_KEY = "lps:prune"
_TRY_LOCK = "SELECT pg_try_advisory_lock(hashtext(%s)) AS locked"
_UNLOCK = "SELECT pg_advisory_unlock(hashtext(%s))"

# SKIP LOCKED passes over a row that another transaction, such as a retry, holds; the
# next pass picks it up if it still qualifies.
_SELECT_BATCH = """
SELECT run_id
FROM runs
WHERE status IN ('success', 'failed')
  AND ended_at < %(cutoff)s
  AND checkpoints_pruned_at IS NULL
ORDER BY ended_at, run_id
LIMIT %(limit)s
FOR UPDATE SKIP LOCKED
"""

_MARK_PRUNED = """
UPDATE runs SET checkpoints_pruned_at = now() WHERE run_id = ANY(%(run_ids)s::uuid[])
"""

_DELETE_RUNS = """
DELETE FROM runs
WHERE status IN ('success', 'failed')
  AND ended_at < %(cutoff)s
  AND checkpoints_pruned_at IS NOT NULL
"""


@dataclass(frozen=True)
class PruneReport:
    """What one ``prune_once`` pass did.

    - ``leader``: whether this pass held the leader lock. A pass without it does nothing.
    - ``threads_pruned``: the runs whose checkpoint threads were pruned.
    - ``rows_deleted``: the ``runs`` rows deleted plus the rows that the agents'
      ``retention_sql`` statements deleted.
    """

    leader: bool
    threads_pruned: int
    rows_deleted: int


async def prune_once(
    pool: _Pool,
    saver: BaseCheckpointSaver[str],
    agents: Mapping[str, AgentSpec],
    settings: Settings,
    *,
    now: datetime,
) -> PruneReport:
    """Make one retention pass, if no other session holds the leader lock (see above).

    ``pool`` is the autocommit pool from ``create_pool``, and ``saver`` the checkpointer
    on it; ``agents`` supply the ``retention_sql`` statements. The cutoffs are ``now``
    minus ``settings.retention_checkpoint_days`` and ``settings.retention_result_days``.
    Returns ``PruneReport(False, 0, 0)`` when another session holds the lock.
    """
    async with _leader_lock(pool) as conn:
        if conn is None:
            return PruneReport(leader=False, threads_pruned=0, rows_deleted=0)
        threads = await _prune_checkpoints(
            conn, saver, now - timedelta(days=settings.retention_checkpoint_days)
        )
        rows = await _delete_results(
            conn, agents, now - timedelta(days=settings.retention_result_days)
        )
    return PruneReport(leader=True, threads_pruned=threads, rows_deleted=rows)


@asynccontextmanager
async def _leader_lock(pool: _Pool) -> AsyncIterator[_Conn | None]:
    """Lend a connection that holds the leader lock, or None if another session has it.

    The lock belongs to the connection's session: a pooled connection that kept it would
    lead for good. So the lock is released before the connection goes back to the pool,
    and a connection on which the lock request or the release fails is closed instead;
    its session ends and takes the lock with it, and the pool replaces it.
    """
    async with pool.connection() as conn:
        try:
            cur = await conn.execute(_TRY_LOCK, [_LOCK_KEY])
            row = await cur.fetchone()
        except BaseException:
            # The server may have granted the lock before the error.
            await conn.close()
            raise
        if not (row and row["locked"]):
            yield None
            return
        try:
            yield conn
        finally:
            await _unlock(conn)


async def _unlock(conn: _Conn) -> None:
    if conn.closed:
        # A lost connection: its session has ended and released the lock.
        return
    try:
        await conn.execute(_UNLOCK, [_LOCK_KEY])
    except BaseException:
        await conn.close()
        raise


async def _prune_checkpoints(conn: _Conn, saver: BaseCheckpointSaver[str], cutoff: datetime) -> int:
    """Prune the checkpoints of every run that qualifies, by batch; return the run count."""
    pruned = 0
    while batch := await _retrying(
        "checkpoints", functools.partial(_prune_batch, conn, saver, cutoff)
    ):
        pruned += batch
    return pruned


async def _prune_batch(conn: _Conn, saver: BaseCheckpointSaver[str], cutoff: datetime) -> int:
    """Prune one batch of runs and mark them; return its size, 0 when none qualify."""
    async with conn.transaction():
        cur = await conn.execute(_SELECT_BATCH, {"cutoff": cutoff, "limit": BATCH_SIZE})
        run_ids = [row["run_id"] for row in await cur.fetchall()]
        if run_ids:
            await saver.aprune([str(run_id) for run_id in run_ids], strategy="delete")
            await conn.execute(_MARK_PRUNED, {"run_ids": run_ids})
    return len(run_ids)


async def _delete_results(conn: _Conn, agents: Mapping[str, AgentSpec], cutoff: datetime) -> int:
    """Run each agent's ``retention_sql``, then delete old runs; return the rows deleted.

    The agents' rows go first, so a table that references ``runs`` never blocks the
    delete of its runs.
    """
    deleted = 0
    params = {"cutoff": cutoff}
    for name in sorted(agents):
        statement = agents[name].retention_sql
        if statement is not None:
            deleted += await _retrying(
                f"agent:{name}", functools.partial(_execute, conn, statement, params)
            )
    deleted += await _retrying("runs", functools.partial(_execute, conn, _DELETE_RUNS, params))
    return deleted


async def _execute(conn: _Conn, statement: str, params: Mapping[str, Any]) -> int:
    """Run one statement in autocommit; return the rows it affected."""
    cur = await conn.execute(statement, params)
    return max(cur.rowcount, 0)


async def _retrying[T](step: str, attempt: Callable[[], Awaitable[T]]) -> T:
    """Await ``attempt()``; after a serialization failure or a deadlock, try it again.

    It tries at most ``MAX_RETRIES`` more times; the last failure propagates. Every
    other error propagates at once.
    """
    for retry in range(1, MAX_RETRIES + 1):
        try:
            return await attempt()
        except _RETRYABLE as exc:
            _log.warning("prune.retry", step=step, retry=retry, error=type(exc).__name__)
            await asyncio.sleep(_RETRY_BACKOFF_S * retry)
    return await attempt()
