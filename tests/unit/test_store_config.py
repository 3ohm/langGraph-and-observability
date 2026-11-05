"""The Store's configuration (P24) that needs no database: the pool is created, never opened."""

from psycopg import Capabilities

from langgraph_and_observability.persistence.pool import create_pool
from langgraph_and_observability.persistence.store import make_store
from langgraph_and_observability.settings import load_settings

DSN = "postgresql://u@db.example/app"


async def test_default_keeps_pipeline_mode():
    store = make_store(create_pool(load_settings(), dsn=DSN))

    # Upstream's own choice: pipeline mode whenever libpq supports it.
    assert store.supports_pipeline is Capabilities().has_pipeline()


async def test_pgbouncer_mode_turns_off_pipeline_mode():
    store = make_store(create_pool(load_settings(), dsn=DSN), pgbouncer_mode=True)

    assert store.supports_pipeline is False
