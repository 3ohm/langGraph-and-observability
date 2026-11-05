"""Process-wide resources that startup creates and code outside the graph reads.

The worker opens the pool once at startup and registers it here with ``set_pool``.
Health checks and other code that runs outside a graph call ``current_pool``.
"""

from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

# Not a constant: set_pool sets it.
_pool: AsyncConnectionPool[AsyncConnection[DictRow]] | None = None  # pylint: disable=invalid-name


def set_pool(pool: AsyncConnectionPool[AsyncConnection[DictRow]] | None) -> None:
    """Register the process's pool. ``None`` clears it, for example at shutdown."""
    global _pool  # pylint: disable=global-statement  # the process-wide registry
    _pool = pool


def current_pool() -> AsyncConnectionPool[AsyncConnection[DictRow]] | None:
    """Return the registered pool, or None before startup and after shutdown."""
    return _pool
