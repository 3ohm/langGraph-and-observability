"""Pool configuration (spec 7.1) that needs no database: the pool is created, never opened."""

import pytest
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.persistence import resources
from langgraph_postgres_starter.persistence.pool import build_conninfo, create_pool
from langgraph_postgres_starter.settings import load_settings

DEFAULTS = {
    "keepalives": "1",
    "keepalives_idle": "30",
    "keepalives_interval": "10",
    "keepalives_count": "3",
    "connect_timeout": "10",
}


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://u:pw@db.example:5433/app",
        "host=db.example port=5433 user=u password=pw dbname=app",
    ],
)
def test_adds_missing_parameters_to_url_and_key_value_dsns(dsn):
    params = conninfo_to_dict(build_conninfo(dsn, load_settings()))
    assert params == {
        "host": "db.example",
        "port": "5433",
        "user": "u",
        "password": "pw",
        "dbname": "app",
        **DEFAULTS,
    }


def test_connect_timeout_comes_from_settings():
    conninfo = build_conninfo("postgresql://u@h/db", load_settings(db_connect_timeout_s=4))
    assert conninfo_to_dict(conninfo)["connect_timeout"] == "4"


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://u@h/db?keepalives_idle=120&connect_timeout=2",
        "host=h user=u dbname=db keepalives_idle=120 connect_timeout=2",
    ],
)
def test_keeps_parameters_the_dsn_already_sets(dsn):
    params = conninfo_to_dict(build_conninfo(dsn, load_settings()))
    assert params["keepalives_idle"] == "120"
    assert params["connect_timeout"] == "2"
    assert params["keepalives_count"] == "3"


def test_leaves_a_complete_dsn_unchanged():
    dsn = (
        "host=h dbname=db keepalives=0 keepalives_idle=1 keepalives_interval=2"
        " keepalives_count=3 connect_timeout=4"
    )
    assert build_conninfo(dsn, load_settings()) == dsn


def test_create_pool_takes_its_settings_and_does_not_open():
    settings = load_settings(
        database_url="postgresql://u@h/db", db_pool_min=2, db_pool_max=7, db_max_lifetime_s=600
    )
    p = create_pool(settings)
    assert p.closed
    assert (p.min_size, p.max_size, p.max_lifetime) == (2, 7, 600)
    assert p._check is AsyncConnectionPool.check_connection
    assert p.conninfo == build_conninfo("postgresql://u@h/db", settings)
    assert p.kwargs == {"autocommit": True, "row_factory": dict_row, "prepare_threshold": 5}


def test_create_pool_dsn_argument_wins_and_pgbouncer_mode_turns_off_prepare():
    settings = load_settings(database_url="postgresql://u@settings/db", pgbouncer_mode=True)
    p = create_pool(settings, dsn="postgresql://u@arg/db")
    assert isinstance(p.conninfo, str) and conninfo_to_dict(p.conninfo)["host"] == "arg"
    assert isinstance(p.kwargs, dict) and p.kwargs["prepare_threshold"] is None


def test_create_pool_needs_a_database_url():
    with pytest.raises(ValueError, match="APP_DATABASE_URL"):
        create_pool(load_settings())


def test_resources_hold_the_process_pool():
    p = create_pool(load_settings(), dsn="postgresql://u@h/db")
    assert resources.current_pool() is None
    resources.set_pool(p)
    try:
        assert resources.current_pool() is p
    finally:
        resources.set_pool(None)
    assert resources.current_pool() is None
