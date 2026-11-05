"""The hosting recipe of docs/guides/customize.md runs a graph on Postgres (F5).

The tests execute the guide's ``host-recipe`` block as it is written, then call its
``run_once`` with the steps graph on a migrated test database.
"""

import re
import uuid
from pathlib import Path
from typing import Any

import pytest

from langgraph_and_observability.persistence.checkpointer import make_checkpointer
from langgraph_and_observability.persistence.migrate import LANGGRAPH, run_migrate
from langgraph_and_observability.persistence.resources import current_pool
from langgraph_and_observability.persistence.serde import make_serde
from langgraph_and_observability.settings import load_settings
from tests.graphs import steps
from tests.graphs.steps import STEPS_SPEC, Fault

pytestmark = pytest.mark.integration

CUSTOMIZE = Path(__file__).resolve().parents[2] / "docs" / "guides" / "customize.md"
# As in tests/unit/test_docs.py, whose test_host_recipe_compiles checks there is one block.
HOST_RECIPE = re.compile(
    r"^<!-- host-recipe:begin -->\n```python\n(.*?)^```\n<!-- host-recipe:end -->$",
    re.DOTALL | re.MULTILINE,
)


class _Tracing:
    """Stands in for the recipe's configure_tracing and close_tracing; records the calls."""

    def __init__(self) -> None:
        self.client = object()
        self.calls: list[str] = []

    def configure_tracing(self, settings):
        self.calls.append("configure")
        return self.client

    async def close_tracing(self, client):
        assert client is self.client
        self.calls.append("close")


def _recipe(tracing: _Tracing) -> dict[str, Any]:
    """Execute the recipe into a fresh namespace, with ``tracing``'s two functions."""
    match = HOST_RECIPE.search(CUSTOMIZE.read_text())
    assert match is not None, "customize.md has no host-recipe block"
    code = compile(match[1], str(CUSTOMIZE), "exec")
    namespace: dict[str, Any] = {}
    exec(code, namespace)  # noqa: S102  # pylint: disable=exec-used  # runs the documented recipe
    # run_once looks these names up in the namespace when it runs.
    namespace["configure_tracing"] = tracing.configure_tracing
    namespace["close_tracing"] = tracing.close_tracing
    return namespace


@pytest.fixture
async def settings(pool, fresh_dsn):
    """Settings for the test database, with the checkpoint and Store tables and test_step_calls.

    The worker's tables are not created: the recipe does not need them.
    """
    settings = load_settings(database_url=fresh_dsn)
    await run_migrate(settings, components={"core", LANGGRAPH})
    await steps.ensure_table(pool)
    return settings


async def _marks(pool, run_id: str) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM test_step_calls WHERE run_id = %s", (run_id,)
        )
        row = await cur.fetchone()
    assert row is not None
    count: int = row["n"]
    return count


async def test_host_recipe_runs_the_steps_graph(pool, settings):
    tracing = _Tracing()
    run_once = _recipe(tracing)["run_once"]
    thread_id = str(uuid.uuid7())

    state = await run_once(STEPS_SPEC, {"key": "recipe"}, settings=settings, thread_id=thread_id)

    assert state == {"key": "recipe", "log": ["a", "b", "c"]}
    saver = make_checkpointer(pool, make_serde(STEPS_SPEC.serde_types))
    checkpoint = await saver.aget_tuple({"configurable": {"thread_id": thread_id}})
    assert checkpoint is not None
    assert checkpoint.checkpoint["channel_values"]["log"] == ["a", "b", "c"]
    # As in the worker, the run's ID is the thread_id, and the pool is registered while the
    # graph runs: each step recorded its two marks in Postgres.
    assert await _marks(pool, thread_id) == 6
    assert current_pool() is None
    assert tracing.calls == ["configure", "close"]


async def test_host_recipe_closes_when_the_graph_fails(pool, settings, monkeypatch):
    monkeypatch.setitem(steps.FAULTS, "broken", Fault(fail_step="b", kind="fatal"))
    tracing = _Tracing()
    run_once = _recipe(tracing)["run_once"]
    thread_id = str(uuid.uuid7())

    with pytest.raises(ValueError, match="injected fatal fault in step b"):
        await run_once(STEPS_SPEC, {"key": "broken"}, settings=settings, thread_id=thread_id)

    assert current_pool() is None
    assert tracing.calls == ["configure", "close"]
