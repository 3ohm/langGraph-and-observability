"""The runs table: one row per agent run, with leases and recovery (spec §7.3, §8; P9).

Postgres owns a run from intake on (§8.1). A pod runs a run under a lease: it sets
``status = 'running'``, ``lease_owner`` to its name and ``lease_expires_at`` to now
plus the lease length (``claim``, ``claim_recoverable``). While the run executes, the
pod extends the lease (``heartbeat``). It ends the attempt with ``mark_success``,
``mark_error`` or ``release``. These four change a run only while it is ``running``
under the caller's lease, so a pod that has lost a lease cannot change the run.

When a pod dies, its leases expire, and another pod's periodic check claims the runs
(§8.3); each such reclaim counts as an attempt (``claim_recoverable``). Lease times
come from the database clock (``now()``), never from the pod's, so pods agree on when
a lease has expired.

The pool is in autocommit mode (``create_pool``): each statement is its own
transaction.
"""

import builtins
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from langgraph_and_observability.ids import new_id
from langgraph_and_observability.logging import get_logger

_log = get_logger(__name__)

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]


class RunStatus(StrEnum):
    """A run's status. A failed attempt goes back to PENDING, or ends the run as FAILED."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class RunRow:  # pylint: disable=too-many-instance-attributes  # one per column of runs
    """One row of ``runs``. ``error`` is the last failed attempt's error."""

    run_id: UUID
    agent: str
    dedup_key: str
    status: RunStatus
    input: dict[str, Any]
    attempt: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    ended_at: datetime | None
    error: dict[str, Any] | None
    checkpoints_pruned_at: datetime | None


_RUN_FIELDS = tuple(f.name for f in fields(RunRow))

_INSERT = """
INSERT INTO runs (run_id, agent, dedup_key, input)
VALUES (%s, %s, %s, %s)
ON CONFLICT (dedup_key) DO NOTHING
RETURNING run_id
"""

# A run can be claimed when it is pending, or running under an expired lease.
_CLAIM = """
UPDATE runs
SET status = 'running',
    lease_owner = %(owner)s,
    lease_expires_at = now() + make_interval(secs => %(lease_s)s),
    started_at = coalesce(started_at, now())
WHERE run_id = %(run_id)s
  AND (status = 'pending' OR (status = 'running' AND lease_expires_at < now()))
RETURNING *
"""

# MATERIALIZED: the candidates are selected and locked once, whatever plan the UPDATE
# gets. SKIP LOCKED passes over the rows that another pod's claim has locked.
# A running candidate lost its lease, so its reclaim counts as an attempt (ruling R71):
# `attempt` is the new count, and `exhausted` marks a run that it fails instead.
_CLAIM_RECOVERABLE = """
WITH candidates AS MATERIALIZED (
    SELECT run_id,
           attempt + (status = 'running')::int AS attempt,
           status = 'running' AND attempt + 1 >= %(max_attempts)s AS exhausted
    FROM runs
    WHERE status = 'pending' OR (status = 'running' AND lease_expires_at < now())
    ORDER BY created_at, run_id
    LIMIT %(limit)s
    FOR UPDATE SKIP LOCKED
), updated AS (
    UPDATE runs r
    SET attempt = c.attempt,
        status = CASE WHEN c.exhausted THEN 'failed' ELSE 'running' END,
        lease_owner = CASE WHEN c.exhausted THEN NULL ELSE %(owner)s END,
        lease_expires_at = CASE
            WHEN c.exhausted THEN NULL
            ELSE now() + make_interval(secs => %(lease_s)s)
        END,
        started_at = coalesce(r.started_at, now()),
        ended_at = CASE WHEN c.exhausted THEN now() ELSE r.ended_at END,
        error = CASE
            WHEN c.exhausted THEN jsonb_build_object(
                'type', 'LeaseExpired',
                'message', format('the run lost its lease %%s times', c.attempt)
            )
            ELSE r.error
        END
    FROM candidates c
    WHERE r.run_id = c.run_id
    RETURNING r.*
)
SELECT * FROM updated ORDER BY created_at, run_id
"""

_HEARTBEAT = """
UPDATE runs
SET lease_expires_at = now() + make_interval(secs => %(lease_s)s)
WHERE run_id = ANY(%(run_ids)s::uuid[]) AND lease_owner = %(owner)s AND status = 'running'
"""

_MARK_SUCCESS = """
UPDATE runs
SET status = 'success', ended_at = now(), lease_owner = NULL, lease_expires_at = NULL
WHERE run_id = %(run_id)s AND lease_owner = %(owner)s AND status = 'running'
"""

# In SET, `attempt` is the value before this update, so `attempt + 1` is the new count.
_MARK_ERROR = """
UPDATE runs
SET attempt = attempt + 1,
    error = %(error)s,
    status = CASE WHEN attempt + 1 >= %(max_attempts)s THEN 'failed' ELSE 'pending' END,
    ended_at = CASE WHEN attempt + 1 >= %(max_attempts)s THEN now() END,
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE run_id = %(run_id)s AND lease_owner = %(owner)s AND status = 'running'
RETURNING status
"""

_RELEASE = """
UPDATE runs
SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
WHERE run_id = ANY(%(run_ids)s::uuid[]) AND lease_owner = %(owner)s AND status = 'running'
"""

_RETRY = """
UPDATE runs
SET status = 'pending', attempt = 0, ended_at = NULL, lease_owner = NULL,
    lease_expires_at = NULL
WHERE run_id = %(run_id)s AND status = 'failed'
"""

_LIST = """
SELECT * FROM runs
WHERE %(status)s::text IS NULL OR status = %(status)s
ORDER BY created_at DESC, run_id DESC
LIMIT %(limit)s
"""


class RunRepo:
    """Reads and changes ``runs`` through ``pool``, an autocommit pool from ``create_pool``.

    ``owner`` is the pod's name and ``lease_s`` a lease length in seconds.
    """

    def __init__(self, pool: _Pool) -> None:
        self._pool = pool

    async def create(
        self,
        agent: str,
        dedup_key: str,
        input: Mapping[str, Any],  # pylint: disable=redefined-builtin  # the runs.input column
    ) -> tuple[UUID, bool]:
        """Add a pending run for ``dedup_key``, unless one exists (spec §8.1 step 3).

        Returns the run's id and True for a new run, or the existing run's id and False.
        A new run gets a uuid7 id. ``input`` must be JSON-serializable; it is stored as
        jsonb.
        """
        run_id = new_id()
        async with self._pool.connection() as conn:
            while True:
                cur = await conn.execute(_INSERT, [run_id, agent, dedup_key, Jsonb(dict(input))])
                if await cur.fetchone() is not None:
                    return run_id, True
                # A new statement, so it sees the conflicting run that the INSERT waited for.
                cur = await conn.execute(
                    "SELECT run_id FROM runs WHERE dedup_key = %s", [dedup_key]
                )
                if (row := await cur.fetchone()) is not None:
                    return row["run_id"], False
                # The conflicting run was deleted in between: insert again.

    async def claim(self, run_id: UUID, owner: str, lease_s: float) -> RunRow | None:
        """Take a lease on one run that is pending, or running under an expired lease.

        Sets the status to running and, on the first claim, ``started_at``. Returns the
        updated run, or None when the run does not exist or cannot be claimed.
        """
        async with self._pool.connection() as conn:
            cur = await conn.execute(_CLAIM, {"run_id": run_id, "owner": owner, "lease_s": lease_s})
            row = await cur.fetchone()
        return None if row is None else _run_row(row)

    async def claim_recoverable(
        self, owner: str, lease_s: float, limit: int, *, max_attempts: int
    ) -> builtins.list[RunRow]:
        """Take a lease on up to ``limit`` claimable runs, oldest first (spec §8.3).

        A run is claimable when it is pending, or running under an expired lease. One
        statement selects the runs ``FOR UPDATE SKIP LOCKED`` and claims them as
        ``claim`` does, so two pods never claim the same run. Returns the claimed runs,
        oldest first.

        A running run whose lease expired was not ended by its pod (OOMKill, SIGKILL,
        node loss), so its reclaim counts as an attempt (ruling R71): ``attempt`` goes
        up by one. When the new count reaches ``max_attempts``, the run is not claimed:
        it is set ``failed``, with ``ended_at`` and the error ``LeaseExpired``, and a
        ``run.failed`` line is logged. A pending run, new or drained (``release``), is
        claimed with ``attempt`` unchanged.
        """
        params = {"owner": owner, "lease_s": lease_s, "limit": limit, "max_attempts": max_attempts}
        async with self._pool.connection() as conn:
            cur = await conn.execute(_CLAIM_RECOVERABLE, params)
            runs = [_run_row(row) for row in await cur.fetchall()]
        for run in runs:
            if run.status == RunStatus.FAILED:
                _log.error(
                    "run.failed",
                    run_id=str(run.run_id),
                    agent=run.agent,
                    attempt=run.attempt,
                    error=run.error,
                )
        return [run for run in runs if run.status == RunStatus.RUNNING]

    async def heartbeat(self, owner: str, run_ids: Sequence[UUID], lease_s: float) -> int:
        """Extend ``owner``'s leases on ``run_ids`` to now plus ``lease_s``.

        Returns how many leases were extended; a run that is missing from the count is
        no longer running under ``owner``'s lease.
        """
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                _HEARTBEAT, {"run_ids": [*run_ids], "owner": owner, "lease_s": lease_s}
            )
            return cur.rowcount

    async def mark_success(self, run_id: UUID, owner: str) -> bool:
        """End a run that ``owner`` holds as success: set ``ended_at``, clear the lease.

        Returns False, and changes nothing, when ``owner`` does not hold the run.
        """
        async with self._pool.connection() as conn:
            cur = await conn.execute(_MARK_SUCCESS, {"run_id": run_id, "owner": owner})
            return cur.rowcount == 1

    async def mark_error(
        self, run_id: UUID, owner: str, error: Mapping[str, Any], max_attempts: int
    ) -> RunStatus:
        """Record a failed attempt of a run that ``owner`` holds (spec §8.2).

        Adds one to ``attempt``, stores ``error`` (JSON-serializable) and clears the
        lease. Returns FAILED, and sets ``ended_at``, when the new count reaches
        ``max_attempts``; otherwise returns PENDING, so that recovery runs it again.

        When ``owner`` does not hold the run, nothing changes and the run's current
        status is returned; LookupError when the run does not exist.
        """
        params = {
            "run_id": run_id,
            "owner": owner,
            "error": Jsonb(dict(error)),
            "max_attempts": max_attempts,
        }
        async with self._pool.connection() as conn:
            cur = await conn.execute(_MARK_ERROR, params)
            row = await cur.fetchone()
            if row is None:
                cur = await conn.execute("SELECT status FROM runs WHERE run_id = %s", [run_id])
                row = await cur.fetchone()
        if row is None:
            raise LookupError(f"run {run_id} does not exist")
        return RunStatus(row["status"])

    async def release(self, run_ids: Sequence[UUID], owner: str) -> int:
        """Return ``owner``'s runs to pending and clear their leases (spec §8.4 step 4).

        ``attempt`` does not change. Returns how many runs were released.
        """
        async with self._pool.connection() as conn:
            cur = await conn.execute(_RELEASE, {"run_ids": [*run_ids], "owner": owner})
            return cur.rowcount

    async def get(self, run_id: UUID) -> RunRow | None:
        """Return the run, or None."""
        async with self._pool.connection() as conn:
            cur = await conn.execute("SELECT * FROM runs WHERE run_id = %s", [run_id])
            row = await cur.fetchone()
        return None if row is None else _run_row(row)

    # In this class's annotations a bare `list` names this method, hence builtins.list.
    async def list(self, status: RunStatus | None = None, limit: int = 50) -> builtins.list[RunRow]:
        """Return up to ``limit`` runs, newest first; only runs in ``status`` if given."""
        params = {"status": None if status is None else status.value, "limit": limit}
        async with self._pool.connection() as conn:
            cur = await conn.execute(_LIST, params)
            return [_run_row(row) for row in await cur.fetchall()]

    async def retry(self, run_id: UUID) -> bool:
        """Return a failed run to pending, with ``attempt = 0`` and no ``ended_at``.

        The last ``error`` stays for the record. Returns False, and changes nothing,
        when the run is not failed.
        """
        async with self._pool.connection() as conn:
            cur = await conn.execute(_RETRY, {"run_id": run_id})
            return cur.rowcount == 1


def _run_row(row: DictRow) -> RunRow:
    """Build a RunRow from a ``runs`` row; columns that RunRow lacks are ignored."""
    values = {name: row[name] for name in _RUN_FIELDS}
    values["status"] = RunStatus(values["status"])
    return RunRow(**values)
