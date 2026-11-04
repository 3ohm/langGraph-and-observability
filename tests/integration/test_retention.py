"""The retention pruner against Postgres (spec 7.4; P6).

Runs get checkpoints from the ``test_steps`` graph, under ``thread_id == str(run_id)``.
Each test sets ``ended_at`` explicitly and passes ``now`` to ``prune_once``.
"""

import asyncio
import dataclasses
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from psycopg import errors as pg_errors

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.persistence.checkpointer import PoolAwarePostgresSaver
from langgraph_postgres_starter.persistence.serde import make_serde
from langgraph_postgres_starter.settings import load_settings
from langgraph_postgres_starter.worker.main import Worker
from langgraph_postgres_starter.worker.retention import PruneReport, prune_once
from langgraph_postgres_starter.worker.runs import RunRepo, RunStatus
from tests.graphs.steps import STEPS_SPEC, StepsInput

pytestmark = pytest.mark.integration

AGENTS = {STEPS_SPEC.name: STEPS_SPEC}
CHECKPOINT_TABLES = ("checkpoints", "checkpoint_blobs", "checkpoint_writes")


class _ScriptedSaver(PoolAwarePostgresSaver):
    """A real saver that records each ``aprune`` call.

    Before it prunes, a call waits for ``gate`` (if set), then raises the next of
    ``errors`` (if any are left).
    """

    def __init__(self, pool, *, errors: Sequence[Exception] = (), gate=None) -> None:
        super().__init__(pool, serde=make_serde(STEPS_SPEC.serde_types))
        self.calls: list[tuple[list[str], str]] = []
        self._errors = list(errors)
        self._gate: asyncio.Event | None = gate

    async def aprune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        self.calls.append((list(thread_ids), strategy))
        if self._gate is not None:
            await self._gate.wait()
        if self._errors:
            raise self._errors.pop(0)
        await super().aprune(thread_ids, strategy=strategy)


@pytest.fixture
def settings(fresh_dsn):
    return load_settings(database_url=fresh_dsn)


@pytest.fixture
async def saver(steps_pool):
    # Async: the saver keeps the running event loop.
    return _ScriptedSaver(steps_pool)


@pytest.fixture
def now():
    return datetime.now(UTC)


async def _run(pool, saver, status: RunStatus, ended_at: datetime | None) -> UUID:
    """A run of the steps graph, with its checkpoints, in ``status``, ended at ``ended_at``."""
    key = uuid.uuid4().hex
    dedup_key = f"test_steps:{key}"
    run_id, created = await RunRepo(pool).create(STEPS_SPEC.name, dedup_key, {"key": key})
    assert created
    model = StepsInput(key=key)
    graph = STEPS_SPEC.build_graph().compile(checkpointer=saver)
    await graph.ainvoke(
        STEPS_SPEC.initial_state(model),
        {"configurable": {"thread_id": str(run_id)}},
        context=STEPS_SPEC.make_context(
            RunInfo(run_id=run_id, dedup_key=dedup_key, attempt=1), model, load_settings()
        ),
        durability="sync",
    )
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE runs SET status = %s, ended_at = %s WHERE run_id = %s",
            [status.value, ended_at, run_id],
        )
    return run_id


async def _checkpoint_rows(pool, run_id: UUID) -> dict[str, int]:
    """The run's rows in each of LangGraph's checkpoint tables (read only)."""
    counts = {}
    async with pool.connection() as conn:
        for table in CHECKPOINT_TABLES:
            cur = await conn.execute(
                f"SELECT count(*) AS n FROM {table} WHERE thread_id = %s",  # noqa: S608
                [str(run_id)],
            )
            row = await cur.fetchone()
            assert row is not None
            counts[table] = row["n"]
    return counts


def _all_present(counts: dict[str, int]) -> bool:
    return all(n > 0 for n in counts.values())


NO_CHECKPOINTS = dict.fromkeys(CHECKPOINT_TABLES, 0)


async def _pruned_at(pool, run_id: UUID) -> datetime | None:
    run = await RunRepo(pool).get(run_id)
    assert run is not None
    return run.checkpoints_pruned_at


async def _advisory_locks(pool) -> int:
    """How many advisory locks any session holds in the test database."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM pg_locks WHERE locktype = 'advisory'"
            " AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
        )
        row = await cur.fetchone()
    assert row is not None
    count: int = row["n"]
    return count


async def test_prunes_only_old_finished_runs(steps_pool, saver, settings, now):
    old_success = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=20))
    old_failed = await _run(steps_pool, saver, RunStatus.FAILED, now - timedelta(days=20))
    recent = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=1))
    assert _all_present(await _checkpoint_rows(steps_pool, old_success))

    report = await prune_once(steps_pool, saver, AGENTS, settings, now=now)

    assert report == PruneReport(leader=True, threads_pruned=2, rows_deleted=0)
    assert [strategy for _, strategy in saver.calls] == ["delete"]
    for run_id in (old_success, old_failed):
        assert await _checkpoint_rows(steps_pool, run_id) == NO_CHECKPOINTS
        assert await _pruned_at(steps_pool, run_id) is not None
    assert _all_present(await _checkpoint_rows(steps_pool, recent))
    assert await _pruned_at(steps_pool, recent) is None
    # The leader lock went back with its connection: no session holds it any more.
    assert await _advisory_locks(steps_pool) == 0

    # A second pass finds nothing left to do.
    again = await prune_once(steps_pool, saver, AGENTS, settings, now=now)
    assert again == PruneReport(leader=True, threads_pruned=0, rows_deleted=0)


async def test_deletes_old_rows_and_runs_agent_sql(steps_pool, saver, settings, now):
    # The foreign key fails the pass if runs rows go before the agent's rows.
    async with steps_pool.connection() as conn:
        await conn.execute(
            "CREATE TABLE test_results"
            " (run_id uuid REFERENCES runs (run_id), created_at timestamptz)"
        )
    spec = dataclasses.replace(
        STEPS_SPEC, retention_sql="DELETE FROM test_results WHERE created_at < %(cutoff)s"
    )
    # 100 days: past result retention (90). 30 days: past checkpoint retention only.
    expired = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=100))
    kept = await _run(steps_pool, saver, RunStatus.FAILED, now - timedelta(days=30))
    async with steps_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO test_results VALUES (%s, %s), (%s, %s)",
            [expired, now - timedelta(days=100), kept, now - timedelta(days=30)],
        )

    report = await prune_once(steps_pool, saver, {spec.name: spec}, settings, now=now)

    # rows_deleted: one runs row plus one test_results row.
    assert report == PruneReport(leader=True, threads_pruned=2, rows_deleted=2)
    repo = RunRepo(steps_pool)
    assert await repo.get(expired) is None
    # The expired run's checkpoints went before its row.
    assert await _checkpoint_rows(steps_pool, expired) == NO_CHECKPOINTS
    kept_run = await repo.get(kept)
    assert kept_run is not None
    assert kept_run.checkpoints_pruned_at is not None
    assert await _checkpoint_rows(steps_pool, kept) == NO_CHECKPOINTS
    async with steps_pool.connection() as conn:
        cur = await conn.execute("SELECT run_id FROM test_results")
        assert await cur.fetchall() == [{"run_id": kept}]
    assert await _advisory_locks(steps_pool) == 0


async def test_keeps_rows_whose_checkpoints_remain(steps_pool, saver, fresh_dsn, now):
    # Result retention shorter than checkpoint retention: a run past the first only keeps
    # its row until its checkpoints are pruned.
    settings = load_settings(
        database_url=fresh_dsn, retention_checkpoint_days=14, retention_result_days=5
    )
    run_id = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=10))

    report = await prune_once(steps_pool, saver, AGENTS, settings, now=now)

    assert report == PruneReport(leader=True, threads_pruned=0, rows_deleted=0)
    assert await RunRepo(steps_pool).get(run_id) is not None
    assert _all_present(await _checkpoint_rows(steps_pool, run_id))


async def test_single_leader(steps_pool, settings, now):
    gate = asyncio.Event()
    saver = _ScriptedSaver(steps_pool, gate=gate)
    run_id = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=20))

    async def prune() -> PruneReport:
        report = await prune_once(steps_pool, saver, AGENTS, settings, now=now)
        if not report.leader:
            # The follower has returned while the leader waits in aprune: let it go on.
            gate.set()
        return report

    # Without a leader lock both calls would wait at the gate, and the timeout fails.
    async with asyncio.timeout(30):
        reports = await asyncio.gather(prune(), prune())

    assert sorted(reports, key=lambda r: r.leader) == [
        PruneReport(leader=False, threads_pruned=0, rows_deleted=0),
        PruneReport(leader=True, threads_pruned=1, rows_deleted=0),
    ]
    assert saver.calls == [([str(run_id)], "delete")]
    assert await _advisory_locks(steps_pool) == 0


async def test_running_runs_never_pruned(steps_pool, saver, settings, now):
    # Unfinished runs, with an ended_at past both retention periods: a status filter only.
    long_ago = now - timedelta(days=100)
    pending = await _run(steps_pool, saver, RunStatus.PENDING, long_ago)
    running = await _run(steps_pool, saver, RunStatus.RUNNING, long_ago)

    report = await prune_once(steps_pool, saver, AGENTS, settings, now=now)

    assert report == PruneReport(leader=True, threads_pruned=0, rows_deleted=0)
    assert saver.calls == []
    for run_id in (pending, running):
        assert await _pruned_at(steps_pool, run_id) is None
        assert _all_present(await _checkpoint_rows(steps_pool, run_id))


async def test_prunes_in_batches_of_100(steps_pool, saver, settings, now):
    # 101 finished runs past checkpoint retention; only the graph's run has checkpoints.
    with_checkpoints = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=20))
    async with steps_pool.connection() as conn:
        await conn.execute(
            "INSERT INTO runs (run_id, agent, dedup_key, status, input, ended_at)"
            " SELECT gen_random_uuid(), 'test_steps', 'bulk:' || i, 'success', '{}',"
            " %s - make_interval(days => 20)"
            " FROM generate_series(1, 100) AS i",
            [now],
        )

    report = await prune_once(steps_pool, saver, AGENTS, settings, now=now)

    assert report == PruneReport(leader=True, threads_pruned=101, rows_deleted=0)
    assert [len(thread_ids) for thread_ids, _ in saver.calls] == [100, 1]
    assert len({t for thread_ids, _ in saver.calls for t in thread_ids}) == 101
    assert await _checkpoint_rows(steps_pool, with_checkpoints) == NO_CHECKPOINTS
    async with steps_pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM runs WHERE checkpoints_pruned_at IS NULL"
        )
        assert await cur.fetchone() == {"n": 0}


async def test_retries_serialization_failure_and_deadlock(steps_pool, settings, now):
    saver = _ScriptedSaver(
        steps_pool, errors=[pg_errors.SerializationFailure(), pg_errors.DeadlockDetected()]
    )
    run_id = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=20))

    report = await prune_once(steps_pool, saver, AGENTS, settings, now=now)

    assert report == PruneReport(leader=True, threads_pruned=1, rows_deleted=0)
    assert len(saver.calls) == 3
    assert await _checkpoint_rows(steps_pool, run_id) == NO_CHECKPOINTS
    assert await _pruned_at(steps_pool, run_id) is not None


async def test_gives_up_after_three_retries(steps_pool, settings, now):
    saver = _ScriptedSaver(steps_pool, errors=[pg_errors.DeadlockDetected() for _ in range(4)])
    run_id = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=20))

    with pytest.raises(pg_errors.DeadlockDetected):
        await prune_once(steps_pool, saver, AGENTS, settings, now=now)

    # The first try and three retries.
    assert len(saver.calls) == 4
    assert await _pruned_at(steps_pool, run_id) is None
    assert _all_present(await _checkpoint_rows(steps_pool, run_id))
    assert await _advisory_locks(steps_pool) == 0


async def test_other_errors_propagate_and_release_the_lock(steps_pool, settings, now):
    saver = _ScriptedSaver(steps_pool, errors=[RuntimeError("boom")])
    run_id = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=20))

    with pytest.raises(RuntimeError, match="boom"):
        await prune_once(steps_pool, saver, AGENTS, settings, now=now)

    assert len(saver.calls) == 1
    assert await _pruned_at(steps_pool, run_id) is None
    assert await _advisory_locks(steps_pool) == 0
    # The pool's connections are free of the lock: the next pass leads and prunes.
    report = await prune_once(steps_pool, saver, AGENTS, settings, now=now)
    assert report == PruneReport(leader=True, threads_pruned=1, rows_deleted=0)


async def test_cancelled_pass_rolls_back_and_releases_the_lock(steps_pool, settings, now):
    # Worker.stop cancels a pass that may be inside a batch.
    saver = _ScriptedSaver(steps_pool, gate=asyncio.Event())
    run_id = await _run(steps_pool, saver, RunStatus.SUCCESS, now - timedelta(days=20))
    task = asyncio.create_task(prune_once(steps_pool, saver, AGENTS, settings, now=now))
    # Wait until the pass waits in aprune, with its batch's transaction open.
    async with asyncio.timeout(10):
        while True:
            if saver.calls:
                break
            await asyncio.sleep(0.01)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await _pruned_at(steps_pool, run_id) is None
    assert _all_present(await _checkpoint_rows(steps_pool, run_id))
    assert await _advisory_locks(steps_pool) == 0


async def test_worker_runs_the_pruner(steps_pool, saver, fresh_dsn):
    run_id = await _run(
        steps_pool, saver, RunStatus.SUCCESS, datetime.now(UTC) - timedelta(days=20)
    )
    worker = Worker(
        load_settings(database_url=fresh_dsn, health_port=0),
        agents=AGENTS,
        sqs_client=None,
        owner="pod-a",
    )
    await worker.start()
    try:
        # The pruner loop makes its first pass at start.
        async with asyncio.timeout(10):
            while True:
                if await _pruned_at(steps_pool, run_id) is not None:
                    break
                await asyncio.sleep(0.05)
    finally:
        await worker.stop()

    assert await _checkpoint_rows(steps_pool, run_id) == NO_CHECKPOINTS
    assert await _advisory_locks(steps_pool) == 0
