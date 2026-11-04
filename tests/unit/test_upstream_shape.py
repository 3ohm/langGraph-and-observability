"""The upstream shape that PoolAwarePostgresSaver overrides (spec 7.2; P3).

PoolAwarePostgresSaver copies the body of AsyncPostgresSaver._cursor and changes only
the lock. These tests fail when that private method changes shape, so the copy is
reviewed again, or deleted once upstream fixes the lock.
"""

import inspect

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver


def test_upstream_cursor_signature_unchanged():
    assert list(inspect.signature(AsyncPostgresSaver._cursor).parameters) == ["self", "pipeline"]


def test_upstream_still_takes_instance_lock():
    # When this fails, upstream fixed the lock:
    # delete PoolAwarePostgresSaver (docs/practices/P3.md).
    assert "async with self.lock" in inspect.getsource(AsyncPostgresSaver._cursor)
