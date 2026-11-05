"""The Postgres connection pool (spec §7.1).

Each process opens one ``AsyncConnectionPool`` at startup and waits for it to fill, so
startup fails when Postgres can't be reached. Every connection is in autocommit mode
and returns rows as dicts. The pool checks each connection before it lends it out, and
replaces a connection that the server or a proxy has closed. ``max_lifetime`` must stay
below the idle timeout of any proxy in front of the database.

With ``APP_PGBOUNCER_MODE=true`` connections never prepare statements
(``prepare_threshold=None``), which PgBouncer's transaction mode needs.
"""

from psycopg import AsyncConnection, OperationalError
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from langgraph_and_observability.settings import Settings

# psycopg's default: a query runs this many times before psycopg prepares it.
_PREPARE_THRESHOLD = 5


class DatabaseUnavailable(RuntimeError):
    """The pool could not connect to Postgres in time."""


def build_conninfo(dsn: str, settings: Settings) -> str:
    """Return ``dsn`` with TCP keepalives and a connect timeout added.

    ``dsn`` is a URL (``postgresql://...``) or a key=value string. A parameter that
    ``dsn`` already sets keeps its value.
    """
    defaults = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 3,
        "connect_timeout": settings.db_connect_timeout_s,
    }
    present = conninfo_to_dict(dsn)
    return make_conninfo(dsn, **{k: v for k, v in defaults.items() if k not in present})


def create_pool(
    settings: Settings, *, dsn: str | None = None
) -> AsyncConnectionPool[AsyncConnection[DictRow]]:
    """Create the pool, not yet open. ``dsn`` defaults to ``settings.database_url``.

    Open it with ``open_pool``. Raises ValueError when there is no DSN.
    """
    dsn = dsn or settings.database_url
    if not dsn:
        raise ValueError("no database URL: set APP_DATABASE_URL")
    return AsyncConnectionPool(
        build_conninfo(dsn, settings),
        open=False,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        max_lifetime=settings.db_max_lifetime_s,
        check=AsyncConnectionPool.check_connection,
        kwargs={
            "autocommit": True,
            "row_factory": dict_row,
            "prepare_threshold": None if settings.pgbouncer_mode else _PREPARE_THRESHOLD,
        },
    )


# The timeout goes to the pool's own open(wait=True), which closes the pool when the
# time runs out. An asyncio.timeout around open() would cancel it instead.
async def open_pool(
    pool: AsyncConnectionPool[AsyncConnection[DictRow]],
    *,
    timeout: float,  # noqa: ASYNC109
) -> None:
    """Open ``pool`` and wait up to ``timeout`` seconds for its first connections.

    Raises DatabaseUnavailable, chained to the psycopg error, when Postgres can't be
    reached in time. The pool is then closed.
    """
    try:
        await pool.open(wait=True, timeout=timeout)
    except (PoolTimeout, OperationalError) as exc:
        # A wait() timeout has closed the pool already; close() is then a no-op.
        await pool.close()
        raise DatabaseUnavailable(
            f"could not connect to Postgres within {timeout} s (pool {pool.name!r})"
        ) from exc
