"""The runs table and its repository against a real Postgres (spec 7.3, 8.1-8.4; P9)."""

import asyncio
import uuid
from datetime import timedelta

import pytest
import structlog

from langgraph_postgres_starter.persistence.migrate import run_migrate
from langgraph_postgres_starter.settings import load_settings
from langgraph_postgres_starter.worker.runs import RunRepo, RunRow, RunStatus

pytestmark = pytest.mark.integration

AGENT = "steps"
INPUT = {"service": "checkout", "alert": {"name": "5xx rate", "threshold": 0.02}}
ERROR = {"type": "RuntimeError", "message": "step b failed"}


@pytest.fixture
async def repo(pool, fresh_dsn):
    """A RunRepo on the test pool, after the core and worker migrations."""
    await run_migrate(load_settings(database_url=fresh_dsn), components={"core", "worker"})
    return RunRepo(pool)


async def _create(repo: RunRepo, n: int = 1) -> list[uuid.UUID]:
    """Create ``n`` runs, one after another, and return their ids (oldest first)."""
    ids = []
    for i in range(n):
        run_id, created = await repo.create(AGENT, f"alert:{i}", INPUT)
        assert created
        ids.append(run_id)
    return ids


async def _get(repo: RunRepo, run_id: uuid.UUID) -> RunRow:
    run = await repo.get(run_id)
    assert run is not None
    return run


async def test_create_is_idempotent_on_dedup_key(repo):
    first = await repo.create(AGENT, "alert:42", INPUT)
    second = await repo.create(AGENT, "alert:42", {"service": "another payload"})

    run_id = first[0]
    assert first == (run_id, True)
    assert second == (run_id, False)
    assert run_id.version == 7
    run = await _get(repo, run_id)
    assert (run.agent, run.dedup_key, run.status, run.input, run.attempt, run.error) == (
        AGENT,
        "alert:42",
        RunStatus.PENDING,
        INPUT,
        0,
        None,
    )
    assert (run.lease_owner, run.lease_expires_at, run.started_at, run.ended_at) == (
        None,
        None,
        None,
        None,
    )


async def test_claim_recoverable_never_double_claims(repo, pool):
    await _create(repo, 10)
    # Open a second connection now, so that the two claims can run at the same time.
    async with pool.connection(), pool.connection():
        pass

    a, b = await asyncio.gather(
        repo.claim_recoverable("pod-a", 60, 10, max_attempts=3),
        repo.claim_recoverable("pod-b", 60, 10, max_attempts=3),
    )

    ids_a, ids_b = {run.run_id for run in a}, {run.run_id for run in b}
    assert ids_a.isdisjoint(ids_b)
    assert len(ids_a | ids_b) == 10
    assert all(run.status is RunStatus.RUNNING and run.lease_owner == "pod-a" for run in a)
    assert all(run.status is RunStatus.RUNNING and run.lease_owner == "pod-b" for run in b)


async def test_claim_recoverable_takes_oldest_first(repo):
    ids = await _create(repo, 3)

    claimed = await repo.claim_recoverable("pod-a", 60, 2, max_attempts=3)

    assert [run.run_id for run in claimed] == ids[:2]
    assert (await _get(repo, ids[2])).status is RunStatus.PENDING


async def test_expired_lease_is_reclaimed(repo):
    by_claim, by_recovery = await _create(repo, 2)
    first = await repo.claim(by_claim, "pod-a", 1)
    await repo.claim(by_recovery, "pod-a", 1)
    assert first is not None and first.started_at is not None

    await asyncio.sleep(1.1)
    taken = await repo.claim(by_claim, "pod-b", 60)
    recovered = await repo.claim_recoverable("pod-c", 60, 10, max_attempts=3)

    assert taken is not None
    assert (taken.status, taken.lease_owner) == (RunStatus.RUNNING, "pod-b")
    assert taken.started_at == first.started_at
    assert [(run.run_id, run.lease_owner) for run in recovered] == [(by_recovery, "pod-c")]
    # pod-a lost both leases: it can no longer extend or end these runs.
    assert await repo.heartbeat("pod-a", [by_claim, by_recovery], 60) == 0
    assert await repo.mark_success(by_recovery, "pod-a") is False


async def test_claim_recoverable_skips_locked_runs(repo, pool):
    ids = await _create(repo, 5)
    locked = ids[:3]
    async with pool.connection() as conn, conn.transaction():
        await conn.execute(
            "SELECT run_id FROM runs WHERE run_id = ANY(%s::uuid[]) FOR UPDATE", [locked]
        )
        # SKIP LOCKED: the claim passes over the rows that another transaction holds, at
        # once, instead of waiting for them.
        claimed = await asyncio.wait_for(
            repo.claim_recoverable("pod-b", 60, 10, max_attempts=3), timeout=2
        )

    assert [run.run_id for run in claimed] == ids[3:]
    assert [(await _get(repo, run_id)).status for run_id in locked] == [RunStatus.PENDING] * 3


async def test_lease_expiry_counts_as_an_attempt(repo):
    # Ruling R71: a run whose lease expired was not ended by its pod (OOMKill, SIGKILL,
    # node loss). Each reclaim counts as an attempt, and the reclaim that reaches
    # max_attempts fails the run instead of claiming it: a run that kills its pod stops.
    [run_id] = await _create(repo)
    # The claim of a pending run is no attempt. A lease of -1 s has expired at once.
    [first] = await repo.claim_recoverable("pod-a", -1, 10, max_attempts=3)
    assert (first.run_id, first.attempt) == (run_id, 0)
    reclaims = []
    for owner in ("pod-b", "pod-c"):
        [run] = await repo.claim_recoverable(owner, -1, 10, max_attempts=3)
        reclaims.append((run.status, run.lease_owner, run.attempt))
    assert reclaims == [(RunStatus.RUNNING, "pod-b", 1), (RunStatus.RUNNING, "pod-c", 2)]

    with structlog.testing.capture_logs() as logs:
        assert await repo.claim_recoverable("pod-d", 60, 10, max_attempts=3) == []

    run = await _get(repo, run_id)
    lease_expired = {"type": "LeaseExpired", "message": "the run lost its lease 3 times"}
    assert (run.status, run.attempt, run.error) == (RunStatus.FAILED, 3, lease_expired)
    assert (run.lease_owner, run.lease_expires_at) == (None, None)
    assert run.ended_at is not None
    [failed] = [entry for entry in logs if entry["event"] == "run.failed"]
    assert failed["log_level"] == "error"
    assert (failed["run_id"], failed["agent"], failed["error"]) == (
        str(run_id),
        AGENT,
        lease_expired,
    )
    # A failed run is not recovered; only `retry` brings it back.
    assert await repo.claim_recoverable("pod-e", 60, 10, max_attempts=3) == []


async def test_drained_run_is_not_counted(repo):
    # A drained run (SIGTERM, spec 8.4) was released by its pod: no attempt is counted,
    # even with max_attempts=1.
    [run_id] = await _create(repo)
    for owner in ("pod-a", "pod-b", "pod-c"):
        [run] = await repo.claim_recoverable(owner, 60, 10, max_attempts=1)
        assert (run.run_id, run.attempt, run.lease_owner) == (run_id, 0, owner)
        assert await repo.release([run_id], owner) == 1

    run = await _get(repo, run_id)
    assert (run.status, run.attempt, run.error) == (RunStatus.PENDING, 0, None)


async def test_live_lease_is_not_reclaimed(repo):
    [run_id] = await _create(repo)
    await repo.claim(run_id, "pod-a", 60)

    assert await repo.claim_recoverable("pod-b", 60, 10, max_attempts=3) == []
    assert await repo.claim(run_id, "pod-b", 60) is None
    assert (await _get(repo, run_id)).lease_owner == "pod-a"


async def test_heartbeat_extends_only_own_runs(repo):
    mine, theirs = await _create(repo, 2)
    a = await repo.claim(mine, "pod-a", 10)
    b = await repo.claim(theirs, "pod-b", 10)
    assert a is not None and a.lease_expires_at is not None
    assert b is not None

    assert await repo.heartbeat("pod-a", [mine, theirs], 100) == 1

    extended = (await _get(repo, mine)).lease_expires_at
    assert extended is not None and extended - a.lease_expires_at > timedelta(seconds=80)
    assert (await _get(repo, theirs)).lease_expires_at == b.lease_expires_at
    assert await repo.heartbeat("pod-a", [], 100) == 0


async def test_mark_error_pending_then_failed(repo):
    [run_id] = await _create(repo)

    statuses = []
    for attempt in range(1, 4):
        assert await repo.claim(run_id, "pod-a", 60) is not None
        error = {**ERROR, "attempt": attempt}
        statuses.append(await repo.mark_error(run_id, "pod-a", error, max_attempts=3))

    assert statuses == [RunStatus.PENDING, RunStatus.PENDING, RunStatus.FAILED]
    run = await _get(repo, run_id)
    assert (run.status, run.attempt, run.error) == (RunStatus.FAILED, 3, {**ERROR, "attempt": 3})
    assert (run.lease_owner, run.lease_expires_at) == (None, None)
    assert run.ended_at is not None
    # A failed run is not recovered; only `retry` brings it back.
    assert await repo.claim_recoverable("pod-b", 60, 10, max_attempts=3) == []


async def test_outcomes_need_the_lease(repo):
    [run_id] = await _create(repo)
    assert await repo.mark_success(run_id, "pod-a") is False  # pending: nobody holds it
    await repo.claim(run_id, "pod-a", 60)

    assert await repo.mark_success(run_id, "pod-b") is False
    # Another pod's error changes nothing; it gets the run's current status back.
    assert await repo.mark_error(run_id, "pod-b", ERROR, max_attempts=1) is RunStatus.RUNNING
    assert await repo.mark_success(run_id, "pod-a") is True

    run = await _get(repo, run_id)
    assert (run.status, run.attempt, run.error) == (RunStatus.SUCCESS, 0, None)
    assert (run.lease_owner, run.lease_expires_at) == (None, None)
    assert run.ended_at is not None
    assert await repo.mark_success(run_id, "pod-a") is False


async def test_release_clears_lease(repo):
    [run_id] = await _create(repo)
    await repo.claim(run_id, "pod-a", 60)
    await repo.mark_error(run_id, "pod-a", ERROR, max_attempts=3)  # attempt 1
    await repo.claim(run_id, "pod-a", 60)

    assert await repo.release([run_id], "pod-b") == 0
    assert await repo.release([run_id], "pod-a") == 1

    run = await _get(repo, run_id)
    assert (run.status, run.lease_owner, run.lease_expires_at) == (RunStatus.PENDING, None, None)
    assert run.attempt == 1
    # Another pod's periodic check resumes it at once (spec 8.4 step 4).
    recovered = await repo.claim_recoverable("pod-b", 60, 10, max_attempts=3)
    assert [run.run_id for run in recovered] == [run_id]


async def test_retry_resets_failed(repo):
    [run_id] = await _create(repo)
    assert await repo.retry(run_id) is False  # pending
    await repo.claim(run_id, "pod-a", 60)
    assert await repo.retry(run_id) is False  # running
    assert await repo.mark_error(run_id, "pod-a", ERROR, max_attempts=1) is RunStatus.FAILED

    assert await repo.retry(run_id) is True

    run = await _get(repo, run_id)
    assert (run.status, run.attempt, run.ended_at) == (RunStatus.PENDING, 0, None)
    assert (run.lease_owner, run.lease_expires_at) == (None, None)
    assert run.error == ERROR  # kept for the record
    assert await repo.retry(run_id) is False


async def test_list_is_newest_first_and_filters_by_status(repo):
    ids = await _create(repo, 3)
    await repo.claim(ids[0], "pod-a", 60)

    assert [run.run_id for run in await repo.list()] == ids[::-1]
    assert [run.run_id for run in await repo.list(limit=2)] == [ids[2], ids[1]]
    assert [run.run_id for run in await repo.list(RunStatus.RUNNING)] == [ids[0]]
    assert [run.run_id for run in await repo.list(status=RunStatus.PENDING)] == [ids[2], ids[1]]
    assert await repo.list(RunStatus.FAILED) == []
    assert await repo.get(uuid.uuid7()) is None
