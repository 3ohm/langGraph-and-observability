"""The worker's main loop against Postgres and ElasticMQ (spec 8.1-8.4, 8.6; P9, P10).

Each test runs a ``Worker`` in this process with the ``test_steps`` graph and short
intervals. Marks are ordered only by time, so the tests compare mark counts per run.
"""

import asyncio
import json
import socket
import time
import uuid
from collections import Counter
from uuid import UUID

import httpx
import pytest

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.persistence.checkpointer import make_checkpointer
from langgraph_postgres_starter.persistence.serde import make_serde
from langgraph_postgres_starter.settings import load_settings
from langgraph_postgres_starter.worker.main import Worker
from langgraph_postgres_starter.worker.registry import load_agents
from langgraph_postgres_starter.worker.runs import RunRepo, RunRow, RunStatus
from tests.graphs import steps
from tests.graphs.steps import STEPS_SPEC, Fault, StepsInput

pytestmark = pytest.mark.integration

OWNER = "pod-a"
# Short long polls, so an idle intake loop polls once a second.
WAIT_S = 1
DRAIN_TIMEOUT_S = 10


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


@pytest.fixture
def queue(sqs) -> str:
    name = f"worker-{uuid.uuid4().hex}"
    url: str = sqs.create_queue(QueueName=name)["QueueUrl"]
    return url


@pytest.fixture
def repo(steps_pool):
    return RunRepo(steps_pool)


@pytest.fixture
async def make_worker(steps_pool, fresh_dsn, sqs, queue):
    """Build a Worker for the steps agent on the test database and queue.

    Keyword arguments override settings. Every worker is stopped after the test.
    """
    workers: list[Worker] = []

    def make(**overrides):
        settings = load_settings(
            **{
                "database_url": fresh_dsn,
                "sqs_queue_url": queue,
                "sqs_wait_seconds": WAIT_S,
                "intake_agent": STEPS_SPEC.name,
                "health_port": 0,
                "lease_seconds": 5,
                "heartbeat_seconds": 1,
                "recovery_interval_seconds": 1,
                "drain_timeout_seconds": DRAIN_TIMEOUT_S,
                **overrides,
            }
        )
        agents = load_agents(settings, include=[STEPS_SPEC])
        worker = Worker(settings, agents=agents, sqs_client=sqs, owner=OWNER)
        workers.append(worker)
        return worker

    yield make
    for worker in workers:
        await worker.stop()


def _send(sqs, queue: str, key: str) -> None:
    sqs.send_message(QueueUrl=queue, MessageBody=json.dumps({"key": key}))


async def _marks(pool, run_id: UUID) -> Counter[str]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT mark FROM test_step_calls WHERE run_id = %s", (str(run_id),)
        )
        return Counter(row["mark"] for row in await cur.fetchall())


async def _run_for_key(repo: RunRepo, key: str) -> RunRow:
    """Wait until intake has recorded the run for ``key``; return it. At most 10 s."""
    async with asyncio.timeout(10):
        while True:
            for run in await repo.list(limit=100):
                if run.dedup_key == f"test_steps:{key}":
                    return run
            await asyncio.sleep(0.05)


async def _wait_for_status(repo: RunRepo, run_id: UUID, status: RunStatus) -> RunRow:
    """Wait until the run has ``status``; return it. At most 10 s."""
    async with asyncio.timeout(10):
        while True:
            run = await repo.get(run_id)
            assert run is not None
            if run.status == status:
                return run
            await asyncio.sleep(0.05)


async def _wait_for_mark(pool, run_id: UUID, mark: str) -> None:
    """Wait until the run has recorded ``mark``. At most 10 s."""
    async with asyncio.timeout(10):
        while True:
            if (await _marks(pool, run_id))[mark]:
                return
            await asyncio.sleep(0.02)


ALL_STEPS_ONCE = Counter(
    {"a:start": 1, "a:done": 1, "b:start": 1, "b:done": 1, "c:start": 1, "c:done": 1}
)


async def test_processes_queued_message(make_worker, sqs, queue, repo, steps_pool):
    port = _free_port()
    worker = make_worker(health_port=port)
    await worker.start()

    sent = time.monotonic()
    _send(sqs, queue, "q1")
    run = await _run_for_key(repo, "q1")
    done = await _wait_for_status(repo, run.run_id, RunStatus.SUCCESS)

    assert time.monotonic() - sent < 10
    assert (done.lease_owner, done.lease_expires_at, done.attempt) == (None, None, 0)
    assert await _marks(steps_pool, run.run_id) == ALL_STEPS_ONCE
    # The health server runs on the worker's loop, on APP_HEALTH_PORT.
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
        ok = await client.get("/ok")
        ready = await client.get("/ready")
    assert ok.status_code == 200
    assert ready.status_code == 200
    assert ready.json() == {
        "ready": True,
        "checks": {"database": True, "pool_open": True, "intake": True},
    }


async def test_recovery_resumes_expired_run(make_worker, repo, steps_pool):
    # A run that a dead pod left behind: a checkpoint after step a, and an expired lease.
    key = "recover"
    model = StepsInput(key=key)
    run_id, created = await repo.create(STEPS_SPEC.name, f"test_steps:{key}", {"key": key})
    assert created
    graph = STEPS_SPEC.build_graph().compile(
        checkpointer=make_checkpointer(steps_pool, make_serde(STEPS_SPEC.serde_types))
    )
    await graph.ainvoke(
        STEPS_SPEC.initial_state(model),
        {"configurable": {"thread_id": str(run_id)}},
        context=STEPS_SPEC.make_context(
            RunInfo(run_id=run_id, dedup_key=f"test_steps:{key}", attempt=1),
            model,
            load_settings(),
        ),
        durability="sync",
        interrupt_after=["step_a"],
    )
    assert await repo.claim(run_id, "dead-pod", -1) is not None
    assert await _marks(steps_pool, run_id) == Counter({"a:start": 1, "a:done": 1})

    worker = make_worker()
    await worker.start()
    done = await _wait_for_status(repo, run_id, RunStatus.SUCCESS)

    assert done.lease_owner is None
    # P9: the worker resumed the run after step a; a did not run again.
    assert await _marks(steps_pool, run_id) == ALL_STEPS_ONCE


async def test_heartbeat_keeps_the_lease(make_worker, sqs, queue, repo, steps_pool):
    steps.FAULTS["hb"] = Fault(delay_s={"b": 4})
    # Recovery runs once, at start: a later recovery would claim the pod's own run again
    # when its lease expires, which renews the lease without a heartbeat.
    worker = make_worker(lease_seconds=2, heartbeat_seconds=1, recovery_interval_seconds=60)
    await worker.start()
    _send(sqs, queue, "hb")
    run = await _run_for_key(repo, "hb")
    await _wait_for_mark(steps_pool, run.run_id, "b:start")

    # Longer than the lease: only heartbeats keep it.
    await asyncio.sleep(3)
    async with steps_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT status, lease_owner, lease_expires_at > now() AS live FROM runs"
            " WHERE run_id = %s",
            (run.run_id,),
        )
        row = await cur.fetchone()
    assert row == {"status": "running", "lease_owner": OWNER, "live": True}
    # Another pod cannot take a run whose lease is live.
    assert await repo.claim(run.run_id, "pod-b", 60) is None

    await _wait_for_status(repo, run.run_id, RunStatus.SUCCESS)
    assert await _marks(steps_pool, run.run_id) == ALL_STEPS_ONCE


async def test_stop_drains_and_releases(make_worker, sqs, queue, repo, steps_pool):
    steps.FAULTS["drain"] = Fault(delay_s={"b": 2})
    worker = make_worker()
    await worker.start()
    _send(sqs, queue, "drain")
    run = await _run_for_key(repo, "drain")
    await _wait_for_mark(steps_pool, run.run_id, "b:start")

    started = time.monotonic()
    await worker.stop()

    assert time.monotonic() - started < DRAIN_TIMEOUT_S
    row = await repo.get(run.run_id)
    assert row is not None
    assert (row.status, row.lease_owner, row.lease_expires_at, row.attempt) == (
        RunStatus.PENDING,
        None,
        None,
        0,
    )
    # P10: the run stopped after its current step, at a checkpoint it can resume from.
    marks = await _marks(steps_pool, run.run_id)
    assert (marks["b:done"], marks["c:start"]) == (1, 0)
    # stop() is idempotent.
    await worker.stop()


async def test_stop_releases_runs_that_outlast_the_drain_timeout(
    make_worker, sqs, queue, repo, steps_pool
):
    steps.FAULTS["stuck"] = Fault(delay_s={"b": 30})
    worker = make_worker(drain_timeout_seconds=1)
    await worker.start()
    _send(sqs, queue, "stuck")
    run = await _run_for_key(repo, "stuck")
    await _wait_for_mark(steps_pool, run.run_id, "b:start")

    started = time.monotonic()
    await worker.stop()

    assert time.monotonic() - started < 5
    row = await repo.get(run.run_id)
    assert row is not None
    assert (row.status, row.lease_owner, row.lease_expires_at, row.attempt) == (
        RunStatus.PENDING,
        None,
        None,
        0,
    )
    assert (await _marks(steps_pool, run.run_id))["b:done"] == 0


async def test_concurrency_limit(make_worker, sqs, queue, steps_pool):
    keys = [f"slow{i}" for i in range(5)]
    for key in keys:
        steps.FAULTS[key] = Fault(delay_s={"b": 1})
    worker = make_worker(worker_concurrency=2)
    await worker.start()
    for key in keys:
        _send(sqs, queue, key)

    most_running = 0
    async with asyncio.timeout(30):
        while True:
            async with steps_pool.connection() as conn:
                cur = await conn.execute("SELECT status, count(*) AS n FROM runs GROUP BY status")
                counts = {row["status"]: row["n"] for row in await cur.fetchall()}
            running = counts.get("running", 0)
            assert running <= 2, f"{running} runs were running at once"
            most_running = max(most_running, running)
            if counts.get("success") == len(keys):
                break
            await asyncio.sleep(0.02)

    assert most_running == 2
