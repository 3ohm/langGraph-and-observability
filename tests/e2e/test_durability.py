"""Durability of a run across worker processes (spec §8.3, §8.4; P10, P11).

Each test starts ``python -m langgraph_postgres_starter worker`` subprocesses
(``helpers.start_worker``) on a fresh database and a new ElasticMQ queue, with the
test-only ``test_steps`` graph: ``step_a``, then ``step_b``, then ``step_c``. A message
on the queue becomes a run. Worker A stops while step b runs:

- by SIGKILL, so none of its shutdown code runs. Worker B claims the run when A's lease
  expires, and resumes it at step b: step a does not run again (P11).
- by SIGTERM. A drains: it lets step b end, returns the run to pending without a
  lease, and exits 0. Worker B resumes the run at step c (P10).

Every worker of a test gets the same faults (``LPS_TEST_FAULTS``), which make step b
slow. The steps' marks in ``test_step_calls`` show which steps ran, and how often. The
tests compare the marks as counts per run, not in order.
"""

import asyncio
import json
import signal
import subprocess
import uuid
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import psycopg
import pytest
from psycopg.rows import dict_row

from langgraph_postgres_starter.persistence.migrate import run_migrate
from langgraph_postgres_starter.persistence.pool import create_pool, open_pool
from langgraph_postgres_starter.settings import load_settings
from tests.e2e.helpers import free_port, marks, start_worker, wait_for
from tests.graphs import steps
from tests.graphs.steps import STEPS_SPEC, Fault, StepsInput

pytestmark = pytest.mark.e2e

# How long a worker may take to start and take a message from the queue.
START_TIMEOUT_S = 60
# How long a run may take to succeed after its second worker starts: the lease of the
# first worker expires (LEASE_S), recovery claims the run, and the remaining steps run.
FINISH_TIMEOUT_S = 60
# Short leases, heartbeats and recovery, so worker B takes over within seconds.
LEASE_S = 3
HEARTBEAT_S = 1
RECOVERY_INTERVAL_S = 1
# Short long polls, so that a stopping worker's last receive ends soon.
SQS_WAIT_S = 1
# The drain timeout stays at its default, 45 s: a drained worker must exit before this.
DRAIN_EXIT_TIMEOUT_S = 50

ALL_STEPS_ONCE = Counter(
    {"a:start": 1, "a:done": 1, "b:start": 1, "b:done": 1, "c:start": 1, "c:done": 1}
)

type StartWorker = Callable[..., subprocess.Popen[bytes]]


async def _prepare(dsn: str) -> None:
    """Apply the core, langgraph and worker migrations, and create test_step_calls."""
    settings = load_settings(database_url=dsn)
    await run_migrate(settings, components={"core", "langgraph", "worker"})
    pool = create_pool(settings)
    await open_pool(pool, timeout=30)
    try:
        await steps.ensure_table(pool)
    finally:
        await pool.close()


@pytest.fixture
def db(fresh_dsn: str) -> str:
    """fresh_dsn, migrated, with the test_step_calls table."""
    asyncio.run(_prepare(fresh_dsn))
    return fresh_dsn


@pytest.fixture
def queue_url(sqs: Any) -> str:
    url: str = sqs.create_queue(QueueName=f"e2e-{uuid.uuid4().hex}")["QueueUrl"]
    return url


@pytest.fixture
def workers(db: str, sqs_endpoint: str, queue_url: str, tmp_path: Path) -> Iterator[StartWorker]:
    """Start workers for ``db`` and ``queue_url``: ``workers(name, faults=...)``.

    Each worker gets its own health port and ``POD_NAME`` (``e2e-<name>``), and writes
    its output to ``tmp_path / "<name>.log"``. Afterwards, the fixture kills every
    worker that still runs and prints each log, which pytest shows when a test fails.
    """
    started: list[tuple[str, subprocess.Popen[bytes], Path]] = []

    def start(name: str, *, faults: Mapping[str, Fault]) -> subprocess.Popen[bytes]:
        env = {
            "APP_ENV": "test",
            "APP_DATABASE_URL": db,
            "APP_EXTRA_AGENTS": json.dumps(["tests.graphs.steps:STEPS_SPEC"]),
            "APP_INTAKE_AGENT": STEPS_SPEC.name,
            "APP_SQS_ENDPOINT_URL": sqs_endpoint,
            "APP_SQS_QUEUE_URL": queue_url,
            "APP_SQS_WAIT_SECONDS": str(SQS_WAIT_S),
            "APP_LEASE_SECONDS": str(LEASE_S),
            "APP_HEARTBEAT_SECONDS": str(HEARTBEAT_S),
            "APP_RECOVERY_INTERVAL_SECONDS": str(RECOVERY_INTERVAL_S),
            "APP_HEALTH_PORT": str(free_port()),
            "POD_NAME": f"e2e-{name}",
            # ElasticMQ accepts any credentials; botocore needs some to sign requests.
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            # As in the image (Dockerfile).
            "LANGGRAPH_STRICT_MSGPACK": "true",
            steps.FAULTS_ENV: steps.faults_json(faults),
        }
        log_path = tmp_path / f"{name}.log"
        proc = start_worker(env, log_path=log_path)
        started.append((name, proc, log_path))
        return proc

    yield start
    for name, proc, log_path in started:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        print(f"--- worker {name}: exit code {proc.returncode}, log {log_path}")
        print(log_path.read_text(errors="replace"))


def _send(sqs: Any, queue_url: str, key: str) -> None:
    sqs.send_message(QueueUrl=queue_url, MessageBody=StepsInput(key=key).model_dump_json())


def _alive(proc: subprocess.Popen[bytes]) -> Literal[True]:
    """True while ``proc`` runs. A worker that has exited fails the test at once."""
    if (code := proc.poll()) is not None:
        raise AssertionError(f"the worker exited with code {code}; its log is printed below")
    return True


def _run_id(dsn: str, key: str) -> UUID | None:
    """The run that intake recorded for the message with ``key``, or None."""
    dedup_key = STEPS_SPEC.dedup_key(StepsInput(key=key), datetime.now(UTC))
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute("SELECT run_id FROM runs WHERE dedup_key = %s", (dedup_key,)).fetchone()
    return None if row is None else row[0]


def _run(dsn: str, run_id: UUID) -> dict[str, Any]:
    """The run's status, attempt count and lease."""
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT status, attempt, lease_owner, lease_expires_at FROM runs WHERE run_id = %s",
            (run_id,),
        ).fetchone()
    assert row is not None
    return row


def _start_run_and_wait_for_step_b(
    workers: StartWorker, sqs: Any, queue_url: str, db: str, key: str, faults: Mapping[str, Fault]
) -> tuple[subprocess.Popen[bytes], UUID]:
    """Start worker A, send the message for ``key``, and wait until step b has started."""
    worker_a = workers("a", faults=faults)
    _send(sqs, queue_url, key)
    run_id = wait_for(lambda: _alive(worker_a) and _run_id(db, key), START_TIMEOUT_S)
    wait_for(lambda: _alive(worker_a) and "b:start" in marks(run_id, dsn=db), START_TIMEOUT_S)
    return worker_a, run_id


def _wait_for_success(worker: subprocess.Popen[bytes], db: str, run_id: UUID) -> None:
    wait_for(lambda: _alive(worker) and _run(db, run_id)["status"] == "success", FINISH_TIMEOUT_S)


def test_kill_9_resumes_at_failed_step(workers, sqs, queue_url, db):
    # Step b sleeps 5 s, so worker A dies in the middle of it.
    key = "kill-9"
    faults = {key: Fault(delay_s={"b": 5})}
    worker_a, run_id = _start_run_and_wait_for_step_b(workers, sqs, queue_url, db, key, faults)

    worker_a.send_signal(signal.SIGKILL)
    assert worker_a.wait(timeout=10) == -signal.SIGKILL
    worker_b = workers("b", faults=faults)
    _wait_for_success(worker_b, db, run_id)

    counts = Counter(marks(run_id, dsn=db))
    # Step a ran once. Step b started twice, once in each worker, and ended once.
    assert counts["a:done"] == 1
    assert counts["b:start"] == 2
    assert counts["c:done"] == 1
    # Every other mark appears once: step a did not even start again.
    assert counts == ALL_STEPS_ONCE + Counter({"b:start": 1})
    # Worker B's reclaim of the run, whose lease A left behind, counted as an attempt.
    assert _run(db, run_id)["attempt"] == 1


def test_sigterm_drains_then_resumes(workers, sqs, queue_url, db):
    # Step b sleeps 3 s, so worker A gets SIGTERM in the middle of it.
    key = "sigterm"
    faults = {key: Fault(delay_s={"b": 3})}
    worker_a, run_id = _start_run_and_wait_for_step_b(workers, sqs, queue_url, db, key, faults)

    worker_a.send_signal(signal.SIGTERM)
    assert worker_a.wait(timeout=DRAIN_EXIT_TIMEOUT_S) == 0
    # A ended step b before it stopped, and did not start step c.
    assert Counter(marks(run_id, dsn=db)) == Counter(
        {"a:start": 1, "a:done": 1, "b:start": 1, "b:done": 1}
    )
    run = _run(db, run_id)
    assert (run["status"], run["lease_owner"], run["lease_expires_at"]) == ("pending", None, None)
    assert run["attempt"] == 0

    worker_b = workers("b", faults=faults)
    _wait_for_success(worker_b, db, run_id)

    counts = Counter(marks(run_id, dsn=db))
    # B resumed at step c: step b ran once, in A.
    assert counts["b:done"] == 1
    assert counts["c:done"] == 1
    assert counts == ALL_STEPS_ONCE
