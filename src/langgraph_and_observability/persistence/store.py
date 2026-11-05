"""The LangGraph Store on the shared connection pool (P24).

The Store is LangGraph's long-term memory: items, by namespace and key, that outlive a
thread. A node reads and writes them through ``runtime.store``. The stock
``AsyncPostgresStore`` already takes no shared lock on a pool: each call checks out its
own connection, so the template uses it as it is.

The Store's tables come from ``run_migrate`` (the ``langgraph`` component), not from
this module.
"""

from langgraph.store.postgres.aio import AsyncPostgresStore
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool


def make_store(pool: AsyncConnectionPool[AsyncConnection[DictRow]]) -> AsyncPostgresStore:
    """Return an ``AsyncPostgresStore`` on ``pool``, with no index and no TTL.

    Call it from the running event loop: the store keeps that loop. Its values are JSON
    in Postgres (``jsonb``): no serde allowlist applies to them, and ``LANGGRAPH_AES_KEY``
    does not encrypt them at rest (it covers checkpoints only). Never put a secret in the
    Store (AGENTS.md rule 3).

    - Semantic search: pass ``index=`` (a ``PostgresIndexConfig``: ``dims``, ``embed``,
      ``fields``) here and to the ``AsyncPostgresStore`` in ``run_migrate``, whose
      ``setup()`` then creates the ``vector`` extension and the ``store_vectors`` table.
      The Compose image (``pgvector/pgvector``) has pgvector.
    - Expiry: pass ``ttl=`` (a ``TTLConfig``), call ``start_ttl_sweeper()`` once the
      store is built, and ``stop_ttl_sweeper()`` before the pool closes. Without a TTL,
      items stay until a node deletes them: the pruner does not touch the Store.
    """
    return AsyncPostgresStore(pool)
