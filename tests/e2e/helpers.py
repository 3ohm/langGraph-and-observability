"""Helpers for the end-to-end tests: worker subprocesses and the steps graph's marks.

A worker is ``python -m langgraph_postgres_starter worker`` in its own process, started
from the repository root (``start_worker``). The tests signal or kill it, and wait for
what it does (``wait_for``). ``marks`` reads what the test-only ``test_steps`` graph
recorded in ``test_step_calls`` (``tests/graphs/steps.py``).
"""

import os
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from uuid import UUID

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[2]

# Variables of the test process that a worker does not inherit, so that its settings come
# from start_worker's env only: the app's settings (APP_, DD_, LANGGRAPH_AES_KEY), AWS
# credentials and profiles, and LangSmith tracing, which would send runs to LangSmith.
_NOT_INHERITED = ("APP_", "DD_", "AWS_", "LANGGRAPH_", "LANGSMITH_", "LANGCHAIN_", "POD_NAME")


def free_port() -> int:
    """A TCP port on 127.0.0.1 that is free now, for a worker's ``APP_HEALTH_PORT``."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def start_worker(env: Mapping[str, str], *, log_path: Path) -> subprocess.Popen[bytes]:
    """Start ``python -m langgraph_postgres_starter worker``; return its process.

    The worker gets the test process's environment without the variables in
    ``_NOT_INHERITED``, then ``env``. It runs with this interpreter, from the repository
    root, with the root on ``PYTHONPATH``, so that ``APP_EXTRA_AGENTS`` can name
    ``tests.graphs.steps:STEPS_SPEC``. Its stdout and stderr, unbuffered, go to
    ``log_path``. The caller must kill the process if it is still running at the end.
    """
    child_env = {
        name: value
        for name, value in os.environ.items()
        if not name.upper().startswith(_NOT_INHERITED)
    }
    child_env.update(env)
    child_env["PYTHONPATH"] = os.pathsep.join(
        path for path in (str(REPO_ROOT), os.environ.get("PYTHONPATH")) if path
    )
    child_env["PYTHONUNBUFFERED"] = "1"
    with log_path.open("ab") as log:
        # The command is fixed: this interpreter and the package's own entry point.
        return subprocess.Popen(  # noqa: S603
            [sys.executable, "-m", "langgraph_postgres_starter", "worker"],
            cwd=REPO_ROOT,
            env=child_env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
        )


def wait_for[T](pred: Callable[[], T | None], timeout: float, *, interval: float = 0.1) -> T:
    """Call ``pred`` until it returns a truthy value, and return that value.

    Raises TimeoutError when ``timeout`` seconds pass first. An exception from ``pred``
    ends the wait.
    """
    deadline = time.monotonic() + timeout
    while True:
        if result := pred():
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"the condition was not met within {timeout} s")
        time.sleep(interval)


def marks(run_id: UUID, *, dsn: str) -> list[str]:
    """The run's marks in ``test_step_calls`` on ``dsn``, in the order they were recorded."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT mark FROM test_step_calls WHERE run_id = %s ORDER BY at", (str(run_id),)
        ).fetchall()
    return [mark for (mark,) in rows]
