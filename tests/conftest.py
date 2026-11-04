"""Shared pytest fixtures for the test suite."""

import os
from collections.abc import Iterator

import pytest

from langgraph_postgres_starter.settings import get_settings


@pytest.fixture(autouse=True)
def _isolate_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Hide the developer's APP_* and DD_* variables and reset the cached settings.

    Settings matches names case-insensitively, so the check does too.
    """
    for name in list(os.environ):
        if name.upper().startswith(("APP_", "DD_")):
            monkeypatch.delenv(name)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
