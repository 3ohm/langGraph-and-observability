import ast
import socket
import ssl
from pathlib import Path

import psycopg
import pydantic
import pytest
from langgraph.errors import GraphBubbleUp, GraphDrained, NodeTimeoutError
from langgraph.types import RetryPolicy, TimeoutPolicy
from psycopg import errors as pg_errors

from langgraph_and_observability import policies
from langgraph_and_observability.policies import default_retry_policy, is_transient, step_timeout

# The vendor packages whose errors only an agent's own predicate may accept: the core
# policy must not depend on them.
VENDOR_PACKAGES = {"aiohttp", "aiosonic", "kubernetes_asyncio"}


def _validation_error() -> pydantic.ValidationError:
    try:
        pydantic.TypeAdapter(int).validate_python("not a number")
    except pydantic.ValidationError as exc:
        return exc
    raise AssertionError("expected a ValidationError")


# The table from the plan (Task 6), with a few subclasses and neighbours added. The
# vendor clients' errors are in the example agent's table (its tests/test_policies.py).
TABLE = [
    # Transient: connection errors and timeouts.
    pytest.param(ConnectionError("reset"), True, id="ConnectionError"),
    pytest.param(ConnectionResetError(), True, id="ConnectionResetError"),
    pytest.param(TimeoutError(), True, id="TimeoutError"),
    # A failed DNS lookup and a TLS connection closed mid-handshake: both OSError only.
    pytest.param(
        socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution"),
        True,
        id="socket-gaierror",
    ),
    pytest.param(
        ssl.SSLEOFError(8, "EOF occurred in violation of protocol"), True, id="SSLEOFError"
    ),
    pytest.param(
        NodeTimeoutError("collect", 30.5, kind="run", run_timeout=30.0),
        True,
        id="langgraph-NodeTimeoutError",
    ),
    # Transient: Postgres.
    pytest.param(pg_errors.SerializationFailure(), True, id="psycopg-SerializationFailure"),
    pytest.param(pg_errors.DeadlockDetected(), True, id="psycopg-DeadlockDetected"),
    pytest.param(psycopg.OperationalError("server closed"), True, id="psycopg-OperationalError"),
    # Programming and data errors.
    pytest.param(ValueError("bad"), False, id="ValueError"),
    pytest.param(_validation_error(), False, id="pydantic-ValidationError"),
    pytest.param(FileNotFoundError("missing"), False, id="FileNotFoundError"),
    # LangGraph control flow.
    pytest.param(GraphDrained(), False, id="langgraph-GraphDrained"),
    pytest.param(GraphBubbleUp(), False, id="langgraph-GraphBubbleUp"),
]


@pytest.mark.parametrize(("exc", "expected"), TABLE)
def test_is_transient_table(exc, expected):
    assert is_transient(exc) is expected


def _imported_packages(path: Path) -> set[str]:
    """The top-level packages that the module at ``path`` imports."""
    packages: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            packages.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            packages.add(node.module.split(".")[0])
    return packages


def test_core_policies_import_no_vendor_sdk():
    # A fork that removes the example agent, and its vendor clients, keeps this module.
    imported = _imported_packages(Path(policies.__file__))
    assert "langgraph" in imported  # the walk finds the imports
    assert imported & VENDOR_PACKAGES == set()


def test_default_policy():
    p = default_retry_policy()
    assert isinstance(p, RetryPolicy)
    assert p.max_attempts == 3 and p.retry_on is is_transient
    assert p.initial_interval == 0.5 and p.backoff_factor == 2.0 and p.jitter is True


def test_default_retry_policy_takes_retry_on():
    def never(_exc: BaseException) -> bool:
        return False

    p = default_retry_policy(retry_on=never)
    assert p.retry_on is never
    # Only the predicate changes.
    assert p._replace(retry_on=is_transient) == default_retry_policy()


def test_step_timeout_caps_one_node_attempt():
    p = step_timeout(30)
    assert isinstance(p, TimeoutPolicy)
    assert p.run_timeout == 30 and p.idle_timeout is None
