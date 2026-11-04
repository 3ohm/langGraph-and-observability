"""Forward-only SQL migrations (spec §7.3).

Each component keeps numbered plain-SQL files in its own folder, named
``NNNN_name.sql``, with versions 1, 2, 3 and no gaps. ``schema_migrations`` records
each applied ``(component, version)``, so no migration runs twice. Never edit a
migration after release; add a new one.

A migration runs in one transaction together with its ``schema_migrations`` row. A
file that contains ``-- migrate:no-transaction`` runs outside a transaction, for
statements such as ``CREATE INDEX CONCURRENTLY``. Postgres runs a file of several
statements as one implicit transaction, so such a file holds one statement. Its row is
written after it finishes, so make it safe to repeat (``IF NOT EXISTS``).

Only one runner works on a database at a time: it holds the advisory lock
``hashtext('lps:migrate')`` for the whole run. A waiting runner polls
``pg_try_advisory_lock``. It does not block in ``pg_advisory_lock``, because a blocked
session keeps a snapshot open, and ``CREATE INDEX CONCURRENTLY`` in the lock holder
waits for that snapshot: a deadlock. The lock belongs to a session, so connect directly
to Postgres, not through PgBouncer in transaction mode. While it waits, the runner logs
a ``migrate.waiting_for_lock`` warning about every 10 seconds.
"""

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.persistence.pool import create_pool, open_pool
from langgraph_postgres_starter.settings import Settings

CORE_MIGRATIONS_DIR = Path(__file__).parent / "migrations"
WORKER_MIGRATIONS_DIR = Path(__file__).parents[1] / "worker" / "migrations"

# The component name of LangGraph's own checkpoint tables (AsyncPostgresSaver.setup()).
LANGGRAPH = "langgraph"
_BUILT_IN = frozenset({"core", LANGGRAPH, "worker"})

_FILE_NAME = re.compile(r"(\d{4})_([a-z0-9_]+)\.sql")
_NO_TRANSACTION = "-- migrate:no-transaction"
_LOCK_KEY = "lps:migrate"
_LOCK_POLL_S = 0.5
# A waiting runner logs once per this many polls: about every 10 s.
_LOCK_POLLS_PER_LOG = 20

# A logging-module logger, not structlog's: the migrate command leaves logging
# unconfigured, so its stdout holds only its results, and a warning goes to stderr.
_log = logging.getLogger(__name__)

_CREATE_SCHEMA_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    component text NOT NULL,
    version integer NOT NULL,
    name text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (component, version)
)
"""

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]


@dataclass(frozen=True)
class Migration:
    """One SQL file. ``transactional`` is False when it has ``-- migrate:no-transaction``."""

    component: str
    version: int
    name: str
    sql: str
    transactional: bool


def discover(sources: Sequence[tuple[str, Path]]) -> list[Migration]:
    """Read the migrations of each ``(component, directory)``: source order, then version.

    A directory that does not exist is skipped, and files other than ``*.sql`` are
    ignored. Raises ValueError when a component is listed twice, or when a component's
    ``.sql`` files are not named ``NNNN_name.sql``, repeat a version, or do not number
    1, 2, 3 without a gap.
    """
    seen: set[str] = set()
    found: list[Migration] = []
    for component, directory in sources:
        if component in seen:
            raise ValueError(f"migration component {component!r} is listed more than once")
        seen.add(component)
        if not directory.is_dir():
            continue
        by_version: dict[int, Migration] = {}
        for path in sorted(directory.glob("*.sql")):
            match = _FILE_NAME.fullmatch(path.name)
            if match is None:
                raise ValueError(
                    f"{path}: a migration file is named NNNN_name.sql "
                    "(four digits, then lowercase letters, digits and _)"
                )
            version, name = int(match[1]), match[2]
            if version in by_version:
                raise ValueError(
                    f"{component}: duplicate migration version {version} "
                    f"({by_version[version].name} and {name})"
                )
            sql = path.read_text(encoding="utf-8")
            by_version[version] = Migration(
                component, version, name, sql, _NO_TRANSACTION not in sql
            )
        versions = sorted(by_version)
        if versions != list(range(1, len(versions) + 1)):
            raise ValueError(
                f"{component}: migration versions start at 1 and have no gap; found {versions}"
            )
        found += (by_version[v] for v in versions)
    return found


async def apply(pool: _Pool, migrations: Sequence[Migration]) -> list[Migration]:
    """Apply, in the given order, each migration that ``schema_migrations`` lacks.

    ``pool`` is an autocommit pool from ``create_pool``. Creates ``schema_migrations``
    when it is missing and holds the migration lock while it works. Returns the
    migrations it applied. A failed migration raises its psycopg error; the migrations
    before it stay applied.
    """
    async with _migration_lock(pool) as conn:
        return await _apply_locked(conn, migrations)


async def run_migrate(
    settings: Settings,
    *,
    components: set[str] | None = None,
    agent_sources: Sequence[tuple[str, Path]] = (),
) -> list[Migration]:
    """Bring the schema up to date and return the SQL migrations that were applied.

    The order is ``core``; ``langgraph``; ``worker``; then each ``(agent, directory)``
    of ``agent_sources``. ``langgraph`` is the stock ``AsyncPostgresSaver.setup()``. It
    keeps its own versions in ``checkpoint_migrations`` and runs on every call, so new
    checkpoint migrations from a library upgrade apply too. ``components`` names the
    components to run; None runs all of them. Connects with ``migration_database_url``,
    else ``database_url``, and holds the migration lock for the whole run.

    Raises ValueError, before connecting, for an unknown component, an agent with a
    built-in component name, a bad migration folder, or no database URL.
    """
    agents = [name for name, _ in agent_sources]
    if clashes := sorted(_BUILT_IN.intersection(agents)):
        raise ValueError(f"agent migration sources use reserved names: {', '.join(clashes)}")
    known = _BUILT_IN.union(agents)
    if components is not None and (unknown := sorted(components - known)):
        raise ValueError(
            f"unknown components: {', '.join(unknown)} (known: {', '.join(sorted(known))})"
        )

    def selected(component: str) -> bool:
        return components is None or component in components

    sources = [("core", CORE_MIGRATIONS_DIR), ("worker", WORKER_MIGRATIONS_DIR), *agent_sources]
    migrations = discover([s for s in sources if selected(s[0])])
    dsn = settings.migration_database_url or settings.database_url
    if not dsn:
        raise ValueError("no database URL: set APP_MIGRATION_DATABASE_URL or APP_DATABASE_URL")

    pool = create_pool(settings, dsn=dsn)
    await open_pool(pool, timeout=settings.db_connect_timeout_s)
    try:
        async with _migration_lock(pool) as conn:
            applied = await _apply_locked(conn, [m for m in migrations if m.component == "core"])
            if selected(LANGGRAPH):
                # The pool's connections are in autocommit mode, which setup() needs for
                # its CREATE INDEX CONCURRENTLY statements.
                await AsyncPostgresSaver(conn).setup()
            applied += await _apply_locked(conn, [m for m in migrations if m.component != "core"])
    finally:
        await pool.close()
    return applied


@asynccontextmanager
async def _migration_lock(pool: _Pool) -> AsyncIterator[AsyncConnection[DictRow]]:
    """Lend one connection that holds the migration lock, with ``schema_migrations`` ready."""
    async with pool.connection() as conn:
        # Poll: the lock lives in Postgres, so no asyncio.Event can signal it (see above).
        polls = 0
        while not await _try_lock(conn):  # noqa: ASYNC110
            polls += 1
            if polls % _LOCK_POLLS_PER_LOG == 0:
                _log.warning("migrate.waiting_for_lock")
            await asyncio.sleep(_LOCK_POLL_S)
        try:
            await conn.execute(_CREATE_SCHEMA_MIGRATIONS)
            yield conn
        finally:
            # A lost connection has released the lock with its session.
            if not conn.broken:
                await conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", [_LOCK_KEY])


async def _try_lock(conn: AsyncConnection[DictRow]) -> bool:
    cur = await conn.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS locked", [_LOCK_KEY])
    row = await cur.fetchone()
    return bool(row and row["locked"])


async def _apply_locked(
    conn: AsyncConnection[DictRow], migrations: Sequence[Migration]
) -> list[Migration]:
    cur = await conn.execute("SELECT component, version FROM schema_migrations")
    done = {(row["component"], row["version"]) for row in await cur.fetchall()}
    applied: list[Migration] = []
    for migration in migrations:
        if (migration.component, migration.version) in done:
            continue
        if migration.transactional:
            async with conn.transaction():
                await _run(conn, migration)
        else:
            await _run(conn, migration)
        applied.append(migration)
    return applied


async def _run(conn: AsyncConnection[DictRow], migration: Migration) -> None:
    # psycopg types a query as LiteralString; a file's text goes as bytes. With no
    # parameters, psycopg sends it whole, so one file may hold several statements.
    await conn.execute(migration.sql.encode())
    await conn.execute(
        "INSERT INTO schema_migrations (component, version, name) VALUES (%s, %s, %s)",
        [migration.component, migration.version, migration.name],
    )
