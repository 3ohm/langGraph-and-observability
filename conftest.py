"""Fixtures for every test directory: tests/ and the agents' tests under src/.

pytest applies a conftest.py to the tests in its directory and below it. This one is at
the repo root, so the agent tests under ``src/langgraph_and_observability/agents/`` get
the same isolation as the tests under ``tests/``. It also registers the Postgres
fixtures of ``tests/fixtures_pg.py`` (``pg_dsn``, ``fresh_dsn``, ``pool``) for every
directory. The queue fixtures and the test graph's fixtures stay in
``tests/conftest.py``.
"""

import os
from collections.abc import Iterator
from typing import Any, cast

import pytest
from langsmith import utils as ls_utils

from langgraph_and_observability.settings import Settings, get_settings

# A plugin module, not a conftest.py: pytest would apply tests/conftest.py to tests/ only.
pytest_plugins = ["tests.fixtures_pg"]

# LangSmith's variables. With LANGSMITH_TRACING=true and a key in the developer's shell, a
# test that starts a Worker would build a real client and upload the test's runs.
_LANGSMITH_PREFIXES = ("LANGSMITH_", "LANGCHAIN_")

# The variables that Settings reads by their vendor name (validation_alias), for example
# LANGGRAPH_AES_KEY, which has neither the APP_ nor the DD_ prefix.
_SETTINGS_ALIASES = frozenset(
    field.validation_alias.upper()
    for field in Settings.model_fields.values()
    if isinstance(field.validation_alias, str)
)


def _clear_langsmith_env_cache() -> None:
    """Make langsmith read its variables afresh: it caches them with lru_cache."""
    cast(Any, ls_utils.get_env_var).cache_clear()
    ls_utils.get_tracer_project.cache_clear()


@pytest.fixture(autouse=True)
def _isolate_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Hide the developer's settings and LangSmith variables; reset the cached values.

    It hides every APP_*, DD_*, LANGSMITH_* and LANGCHAIN_* variable and every other
    name that Settings reads (``_SETTINGS_ALIASES``). Settings matches names
    case-insensitively, so the check does too. It clears the cached settings and
    langsmith's cache of its variables, before and after each test.
    """
    for name in list(os.environ):
        upper = name.upper()
        if upper.startswith(("APP_", "DD_", *_LANGSMITH_PREFIXES)) or upper in _SETTINGS_ALIASES:
            monkeypatch.delenv(name)
    get_settings.cache_clear()
    _clear_langsmith_env_cache()
    yield
    get_settings.cache_clear()
    _clear_langsmith_env_cache()


@pytest.fixture(params=[False, True], ids=["permissive-msgpack", "strict-msgpack"])
def strict_msgpack(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> bool:
    """Run the test with LANGGRAPH_STRICT_MSGPACK off, then on (the image turns it on).

    LangGraph reads the variable once, at import, into two module constants: the
    serializer's default allowlist reads one, and StateGraph.compile() the other. The
    fixture sets the variable and patches both constants.
    """
    # Imported here: both modules are private. If an upgrade moves one, only the tests
    # that use this fixture fail.
    import langgraph._internal._serde
    import langgraph.checkpoint.serde._msgpack

    strict: bool = request.param
    monkeypatch.setenv("LANGGRAPH_STRICT_MSGPACK", str(strict).lower())
    monkeypatch.setattr(langgraph.checkpoint.serde._msgpack, "STRICT_MSGPACK_ENABLED", strict)
    monkeypatch.setattr(langgraph._internal._serde, "STRICT_MSGPACK_ENABLED", strict)
    return strict
