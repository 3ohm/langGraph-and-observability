"""The action ledger: run a side effect once per idempotency key (spec §7.3, §9.9; P13).

``run_once`` records each finished action in ``action_ledger`` under its key, with a
hash of its arguments and the result it returned. A later call with the same key and
arguments gets the recorded result back and does not run the action again.

Exactly once covers the record, not the outside world: the external effect is
at-least-once (see ``run_once``). Where the receiving system takes an idempotency key,
give it the ledger key too.
"""

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]


@dataclass(frozen=True)
class LedgerOutcome:
    """The action's result. ``replayed`` is True when it came from the ledger."""

    result: dict[str, Any]
    replayed: bool


class LedgerConflict(RuntimeError):
    """The key is already recorded with different arguments."""


async def run_once(
    pool: _Pool,
    *,
    key: str,
    action: str,
    args: Mapping[str, Any],
    fn: Callable[[], Awaitable[dict[str, Any]]],
) -> LedgerOutcome:
    """Run ``fn()`` unless ``key`` is recorded; record its result; return the outcome.

    Everything happens in one transaction on one connection from ``pool`` (an
    autocommit pool from ``create_pool``). The transaction holds
    ``pg_advisory_xact_lock(hashtextextended(key, 0))``, so calls with the same key, in
    any process, run one at a time. With the lock held it reads the key's row:

    - a row with the same arguments: returns its result with ``replayed=True``;
    - a row with other arguments: raises LedgerConflict;
    - no row: awaits ``fn()``, inserts the row, and returns ``replayed=False``.

    ``fn()`` runs while the lock and the connection are held, so keep it short and give
    it its own timeout. If ``fn()`` raises, nothing is recorded and its exception
    propagates unchanged. If the process dies between ``fn()`` and the commit, nothing
    is recorded either, and the next call runs ``fn()`` again: the external effect is
    at-least-once.

    ``args`` and the result must be JSON-serializable. The ledger stores the SHA-256 of
    the arguments as canonical JSON (sorted keys), and the result as jsonb; a replay
    returns the result as read back from jsonb.
    """
    args_hash = _hash_args(args)
    async with pool.connection() as conn, conn.transaction():
        # The lock is a statement of its own. Under READ COMMITTED (the Postgres default)
        # the SELECT below then takes its snapshot after the lock is granted, so it sees
        # the row that the previous holder of the lock committed.
        await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", [key])
        cur = await conn.execute(
            "SELECT args_hash, result FROM action_ledger WHERE key = %s", [key]
        )
        row = await cur.fetchone()
        if row is not None:
            if row["args_hash"] != args_hash:
                raise LedgerConflict(f"ledger key {key!r} is recorded with other arguments")
            return LedgerOutcome(row["result"], replayed=True)
        result = await fn()
        await conn.execute(
            "INSERT INTO action_ledger (key, action, args_hash, result) VALUES (%s, %s, %s, %s)",
            [key, action, args_hash, Jsonb(result)],
        )
    return LedgerOutcome(result, replayed=False)


def _hash_args(args: Mapping[str, Any]) -> str:
    """Return the SHA-256 hex digest of ``args`` as canonical JSON."""
    canonical = json.dumps(dict(args), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()
