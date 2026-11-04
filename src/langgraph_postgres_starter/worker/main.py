"""The queue worker: intake, heartbeats, recovery and drain (spec §8; P9, P10).

``Worker.start`` configures tracing (``observability.langsmith.configure_tracing``; off
unless LangSmith tracing and a key are set), opens the pool, builds the checkpointer and
the run executor, which gives each run its trace metadata (``run_metadata``), starts the
health server on ``APP_HEALTH_PORT`` (§8.6) and starts these loops:

- intake, only when ``APP_SQS_QUEUE_URL`` is set: while a run slot is free, it
  long-polls SQS for at most as many messages as there are free slots, then claims and
  starts the runs that the messages created (§8.1);
- recovery: once at start, then every ``APP_RECOVERY_INTERVAL_SECONDS``, it claims up
  to the free slots' worth of runs that are pending or whose lease has expired (§8.3);
- heartbeat: every ``APP_HEARTBEAT_SECONDS``, it extends the leases of this pod's runs
  (§8.2);
- pruner: once at start, then every ``PRUNE_INTERVAL_S``, it makes a retention pass
  (``retention.prune_once``), which only the pod that holds the leader lock carries out
  (§7.4).

At most ``APP_WORKER_CONCURRENCY`` runs execute at once, each in its own task. A run
that intake recorded but could not start, because recovery took the slot in the
meantime, stays pending for the next recovery. A loop that fails logs the error and
goes on. A run task that fails logs ``run.crashed``; its run keeps the lease until the
lease expires and recovery claims it again, which counts as an attempt.

``Worker.stop`` drains (§8.4): it stops intake, recovery and the pruner, asks every
active run to stop after its current step, waits up to ``APP_DRAIN_TIMEOUT_SECONDS``,
cancels the runs that are still going, and returns this pod's remaining runs to
pending. Then it stops the health server, closes the pool and flushes the traces.

``main`` is the ``worker`` command. It runs a Worker until SIGTERM or SIGINT.

Logs name runs, counts and error types, never the settings, a DSN or a run's input.
"""

import argparse
import asyncio
import contextlib
import functools
import os
import signal
import socket
import time
from collections.abc import Generator, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import uvicorn
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError

from langgraph_postgres_starter.agent_spec import AgentSpec
from langgraph_postgres_starter.logging import configure_logging, get_logger
from langgraph_postgres_starter.observability.langsmith import (
    TracingClient,
    close_tracing,
    configure_tracing,
)
from langgraph_postgres_starter.observability.metadata import run_metadata
from langgraph_postgres_starter.persistence.checkpointer import (
    PoolAwarePostgresSaver,
    make_checkpointer,
)
from langgraph_postgres_starter.persistence.pool import create_pool, open_pool
from langgraph_postgres_starter.persistence.resources import set_pool
from langgraph_postgres_starter.persistence.serde import make_serde
from langgraph_postgres_starter.settings import Settings, load_settings
from langgraph_postgres_starter.worker.executor import RunExecutor, error_fields
from langgraph_postgres_starter.worker.health import ReadinessProbe, create_health_app
from langgraph_postgres_starter.worker.intake import SqsIntake, make_sqs_client
from langgraph_postgres_starter.worker.registry import load_agents
from langgraph_postgres_starter.worker.retention import prune_once
from langgraph_postgres_starter.worker.runs import RunRepo, RunRow

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]

_log = get_logger(__name__)

PROG = "python -m langgraph_postgres_starter worker"

# The exit code when the worker cannot start, as for the CLI's setup errors.
EXIT_STARTUP_FAILED = 2

# How often the pruner loop makes a retention pass (spec §7.4).
PRUNE_INTERVAL_S = 3600.0
# How long the intake loop waits after an error before it polls again.
_ERROR_BACKOFF_S = 1.0
# /ready allows a long poll this many seconds beyond APP_SQS_WAIT_SECONDS.
_POLL_MARGIN_S = 10.0
# The kubelet probes the pod's IP, so the health server listens on every interface.
_HEALTH_HOST = "0.0.0.0"  # noqa: S104
# How long the health server waits for open connections when it stops.
_HEALTH_SHUTDOWN_TIMEOUT_S = 5
_SIGNALS = (signal.SIGTERM, signal.SIGINT)


class _HealthServer(uvicorn.Server):
    """uvicorn's server without its own SIGTERM and SIGINT handlers.

    ``uvicorn.Server.serve`` replaces those handlers while it runs; the worker's
    handlers, installed by ``main``, must stay in place to drain.
    """

    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None]:
        yield


class Worker:
    """One pod's queue worker, for the agents in ``agents`` (``load_agents``).

    ``owner`` is the pod's name, which its leases carry. ``sqs_client`` is a boto3 SQS
    client (``make_sqs_client``); it is needed when ``settings.sqs_queue_url`` is set,
    and unused otherwise.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        agents: Mapping[str, AgentSpec],
        sqs_client: Any | None,
        owner: str,
    ) -> None:
        self._settings = settings
        self._agents = dict(agents)
        self._sqs_client = sqs_client
        self._owner = owner
        self._started = False
        self._tracing: TracingClient | None = None
        self._pool: _Pool | None = None
        self._repo: RunRepo | None = None
        self._executor: RunExecutor | None = None
        self._intake: SqsIntake | None = None
        self._health: _HealthServer | None = None
        self._health_task: asyncio.Task[None] | None = None
        self._intake_task: asyncio.Task[None] | None = None
        self._recovery_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._prune_task: asyncio.Task[None] | None = None
        self._stop_task: asyncio.Task[None] | None = None
        # The run tasks by run, from the claim until the task ends.
        self._runs: dict[UUID, asyncio.Task[None]] = {}
        # Held while a loop counts free slots, claims runs and starts them, so that two
        # loops never fill one slot; stop() takes it to end claiming.
        self._claim_lock = asyncio.Lock()
        self._slot_freed = asyncio.Event()
        self._draining = False
        # For /ready: the intake loop has made progress since this time (its start, or
        # the end of a wait for a free slot), and whether it waits for a slot now.
        self._intake_since = float("-inf")
        self._intake_waiting = False

    async def start(self) -> None:
        """Open the pool, start the health server and the loops; return once they run.

        Raises ValueError when ``sqs_queue_url`` is set but no agent in ``agents`` is
        named ``intake_agent`` or there is no SQS client, before connecting to anything;
        DatabaseUnavailable when Postgres can't be reached within
        ``db_connect_timeout_s``; and RuntimeError when the health server cannot listen.
        A start that fails closes what it opened.
        """
        if self._started:
            raise RuntimeError("a Worker starts only once")
        self._started = True
        intake_agent = self._intake_agent()
        try:
            await self._start(intake_agent)
        except BaseException:
            await self.stop()
            raise
        _log.info(
            "worker.started",
            owner=self._owner,
            agents=sorted(self._agents),
            intake_agent=None if intake_agent is None else intake_agent.name,
            concurrency=self._settings.worker_concurrency,
        )

    async def stop(self) -> None:
        """Drain the runs and shut down (spec §8.4). Returns once the pool and tracing close.

        Safe to call more than once, before ``start`` and while another ``stop`` runs:
        each call waits for the same shutdown. Cancelling a caller does not cancel the
        shutdown.
        """
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._shutdown(), name="worker-stop")
        await asyncio.shield(self._stop_task)

    def _intake_agent(self) -> AgentSpec | None:
        if self._settings.sqs_queue_url is None:
            return None
        name = self._settings.intake_agent
        spec = self._agents.get(name)
        if spec is None:
            registered = ", ".join(sorted(self._agents)) or "none"
            raise ValueError(
                f"APP_INTAKE_AGENT names agent {name!r}, which is not registered "
                f"(registered: {registered}); register it or set APP_INTAKE_AGENT"
            )
        if self._sqs_client is None:
            raise ValueError("APP_SQS_QUEUE_URL is set, but the worker has no SQS client")
        return spec

    async def _start(self, intake_agent: AgentSpec | None) -> None:
        settings = self._settings
        self._tracing = configure_tracing(settings)
        pool = create_pool(settings)
        await open_pool(pool, timeout=settings.db_connect_timeout_s)
        self._pool = pool
        set_pool(pool)
        serde_types = dict.fromkeys(t for spec in self._agents.values() for t in spec.serde_types)
        aes_key = settings.langgraph_aes_key
        serde = make_serde(
            list(serde_types), aes_key=None if aes_key is None else aes_key.get_secret_value()
        )
        saver = make_checkpointer(pool, serde, pgbouncer_mode=settings.pgbouncer_mode)
        repo = RunRepo(pool)
        self._repo = repo
        executor = RunExecutor(
            saver=saver,
            agents=self._agents,
            repo=repo,
            settings=settings,
            owner=self._owner,
            metadata=functools.partial(run_metadata, settings=settings),
        )
        self._executor = executor
        queue_url, client = settings.sqs_queue_url, self._sqs_client
        # With an intake agent, _intake_agent() has checked that both are set.
        if intake_agent is not None and queue_url is not None and client is not None:
            self._intake = SqsIntake(
                client=client,
                queue_url=queue_url,
                agent=intake_agent,
                repo=repo,
                wait_seconds=settings.sqs_wait_seconds,
            )
        await self._start_health(pool)
        self._recovery_task = asyncio.create_task(
            self._recovery_loop(repo, executor), name="worker-recovery"
        )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(repo), name="worker-heartbeat"
        )
        self._prune_task = asyncio.create_task(self._prune_loop(pool, saver), name="worker-prune")
        if self._intake is not None:
            self._intake_since = time.monotonic()
            self._intake_task = asyncio.create_task(
                self._intake_loop(self._intake, repo, executor), name="worker-intake"
            )

    async def _start_health(self, pool: _Pool) -> None:
        probe = ReadinessProbe(pool, intake_alive=self._intake_alive)
        config = uvicorn.Config(
            create_health_app(probe),
            host=_HEALTH_HOST,
            port=self._settings.health_port,
            http="h11",
            ws="none",
            lifespan="off",
            # Keep the JSON logging that configure_logging set up; no access log lines.
            log_config=None,
            access_log=False,
            timeout_graceful_shutdown=_HEALTH_SHUTDOWN_TIMEOUT_S,
        )
        server = _HealthServer(config)
        self._health = server
        task = asyncio.create_task(self._serve_health(server), name="worker-health")
        self._health_task = task
        while not server.started:
            if task.done():
                self._health_task = None
                task.result()
                raise RuntimeError("the health server stopped while it was starting")
            await asyncio.sleep(0.01)

    async def _serve_health(self, server: _HealthServer) -> None:
        try:
            await server.serve()
        except SystemExit as exc:
            # uvicorn exits when it cannot listen. SystemExit must not leave a task: the
            # event loop would re-raise it and stop.
            raise RuntimeError(
                f"the health server could not listen on port {self._settings.health_port}"
            ) from exc

    def _intake_alive(self) -> bool:
        """The readiness check of the intake loop (spec §8.6).

        True without SQS. Otherwise the loop must run, and either wait for a free slot
        now, or have completed a poll, started, or got a slot within one long poll plus
        a margin. Before the first poll completes, only the loop's start counts.
        """
        if self._settings.sqs_queue_url is None:
            return True
        task, intake = self._intake_task, self._intake
        if task is None or task.done() or intake is None:
            return False
        if self._intake_waiting:
            return True
        # last_poll_monotonic starts at the intake's construction, before _intake_since.
        since = max(self._intake_since, intake.last_poll_monotonic)
        return time.monotonic() - since <= self._settings.sqs_wait_seconds + _POLL_MARGIN_S

    def _free_slots(self) -> int:
        return self._settings.worker_concurrency - len(self._runs)

    async def _intake_loop(self, intake: SqsIntake, repo: RunRepo, executor: RunExecutor) -> None:
        while True:
            free = await self._wait_for_free_slots()
            try:
                run_ids = await intake.poll(free)
                await self._claim_new(run_ids, repo, executor)
            except Exception as exc:
                _log.error("intake.failed", error=_error(exc))
                await asyncio.sleep(_ERROR_BACKOFF_S)

    async def _wait_for_free_slots(self) -> int:
        """Wait until a run slot is free; return the number of free slots."""
        if self._free_slots() <= 0:
            self._intake_waiting = True
            try:
                while self._free_slots() <= 0:
                    self._slot_freed.clear()
                    await self._slot_freed.wait()
            finally:
                self._intake_waiting = False
            self._intake_since = time.monotonic()
        return self._free_slots()

    async def _claim_new(
        self, run_ids: Sequence[UUID], repo: RunRepo, executor: RunExecutor
    ) -> None:
        """Claim and start the runs that intake created, while slots are free.

        A run that finds no free slot stays pending, for the recovery loop.
        """
        if not run_ids:
            return
        async with self._claim_lock:
            for run_id in run_ids:
                if self._draining or self._free_slots() <= 0:
                    return
                run = await repo.claim(run_id, self._owner, self._settings.lease_seconds)
                if run is not None:
                    self._start_run(run, executor)

    async def _recovery_loop(self, repo: RunRepo, executor: RunExecutor) -> None:
        while True:
            try:
                await self._recover(repo, executor)
            except Exception as exc:
                _log.error("recovery.failed", error=_error(exc))
            await asyncio.sleep(self._settings.recovery_interval_seconds)

    async def _recover(self, repo: RunRepo, executor: RunExecutor) -> None:
        async with self._claim_lock:
            free = self._free_slots()
            if self._draining or free <= 0:
                return
            runs = await repo.claim_recoverable(
                self._owner,
                self._settings.lease_seconds,
                free,
                max_attempts=self._settings.max_attempts,
            )
            for run in runs:
                self._start_run(run, executor)
        if runs:
            _log.info("recovery.claimed", runs=len(runs))

    async def _heartbeat_loop(self, repo: RunRepo) -> None:
        while True:
            await asyncio.sleep(self._settings.heartbeat_seconds)
            run_ids = list(self._runs)
            if not run_ids:
                continue
            try:
                extended = await repo.heartbeat(self._owner, run_ids, self._settings.lease_seconds)
            except Exception as exc:
                _log.error("heartbeat.failed", runs=len(run_ids), error=_error(exc))
                continue
            if extended < len(run_ids):
                # Also when a run ends while its heartbeat is on the way.
                _log.warning("heartbeat.not_extended", runs=len(run_ids), extended=extended)

    async def _prune_loop(self, pool: _Pool, saver: PoolAwarePostgresSaver) -> None:
        while True:
            try:
                report = await prune_once(
                    pool, saver, self._agents, self._settings, now=datetime.now(UTC)
                )
            except Exception as exc:
                _log.error("prune.failed", error=_error(exc))
            else:
                _log.info(
                    "prune.done",
                    leader=report.leader,
                    threads_pruned=report.threads_pruned,
                    rows_deleted=report.rows_deleted,
                )
            await asyncio.sleep(PRUNE_INTERVAL_S)

    def _start_run(self, run: RunRow, executor: RunExecutor) -> None:
        if run.run_id in self._runs:
            # This pod's own run, claimed again after a heartbeat missed: it still runs.
            return
        self._runs[run.run_id] = asyncio.create_task(
            self._execute(run, executor), name=f"run-{run.run_id}"
        )

    async def _execute(self, run: RunRow, executor: RunExecutor) -> None:
        try:
            # A run whose task had not begun when stop() started is released by stop().
            if not self._draining:
                await executor.execute(run)
        except Exception as exc:
            _log.error("run.crashed", run_id=str(run.run_id), agent=run.agent, error=_error(exc))
        finally:
            self._runs.pop(run.run_id, None)
            self._slot_freed.set()

    async def _shutdown(self) -> None:
        # 1. Stop intake, recovery and the pruner. Under the lock, no claim is half done,
        # and none starts after it. A pass that is cut short rolls back its batch and
        # releases the leader lock.
        async with self._claim_lock:
            self._draining = True
        executor = self._executor
        active = list(self._runs)
        if executor is not None:
            active = list(dict.fromkeys([*active, *executor.active_run_ids()]))
        await _cancel([self._intake_task, self._recovery_task, self._prune_task])
        # 2-3. Ask each run to stop after its current step; wait for the runs to end.
        if executor is not None:
            executor.request_drain_all("shutdown")
        if tasks := list(self._runs.values()):
            _, late = await asyncio.wait(tasks, timeout=self._settings.drain_timeout_seconds)
            if late:
                _log.warning("worker.drain_timeout", runs=len(late))
                await _cancel(late)
        # 4. Return this pod's runs that are still running to pending, without a lease.
        if active and self._repo is not None:
            try:
                released = await self._repo.release(active, self._owner)
            except Exception as exc:
                _log.error("worker.release_failed", runs=len(active), error=_error(exc))
            else:
                _log.info("worker.drained", runs=len(active), released=released)
        await _cancel([self._heartbeat_task])
        # 5. Stop the health server, close the pool and flush the traces. A pool that
        # fails to close is still unregistered, and the traces are still flushed.
        await self._stop_health()
        pool = self._pool
        try:
            if pool is not None:
                await pool.close()
        finally:
            if pool is not None:
                set_pool(None)
            if self._tracing is not None:
                await close_tracing(self._tracing)
        _log.info("worker.stopped", owner=self._owner)

    async def _stop_health(self) -> None:
        if self._health is not None:
            self._health.should_exit = True
        if self._health_task is not None:
            try:
                await self._health_task
            except Exception as exc:
                _log.error("health.failed", error=_error(exc))


async def _cancel(tasks: Iterable[asyncio.Task[Any] | None]) -> None:
    """Cancel ``tasks`` (None entries are skipped) and wait until each has ended."""
    live = [task for task in tasks if task is not None]
    for task in live:
        task.cancel()
    await asyncio.gather(*live, return_exceptions=True)


def _error(exc: BaseException) -> dict[str, str]:
    """An exception as a log field: its type, and its redacted message (``error_fields``)."""
    return error_fields(exc)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the worker until SIGTERM or SIGINT, then drain. Returns the exit code.

    Settings come from the environment. The lease owner is ``POD_NAME`` (Kubernetes'
    downward API), else the host name. Returns 0 after the drain, or
    EXIT_STARTUP_FAILED, with a ``worker.startup_failed`` log line, when the worker
    cannot start: invalid settings, a missing secret, an agent that cannot be loaded,
    an unknown intake agent, or a database that can't be reached.
    """
    argparse.ArgumentParser(
        prog=PROG,
        description="Run the queue worker until SIGTERM or SIGINT. Settings come from the "
        "environment; .env.example lists them.",
    ).parse_args(argv)
    try:
        settings = load_settings()
    except ValidationError as exc:
        # Logging takes settings; these defaults stand in for the invalid ones.
        configure_logging(Settings.model_construct())
        return _startup_failed(exc)
    configure_logging(settings)
    try:
        settings.require_runtime_secrets()
        agents = load_agents(settings)
        sqs_client = None if settings.sqs_queue_url is None else make_sqs_client(settings)
    except Exception as exc:
        return _startup_failed(exc)
    owner = os.environ.get("POD_NAME") or socket.gethostname()
    worker = Worker(settings, agents=agents, sqs_client=sqs_client, owner=owner)
    return asyncio.run(_run(worker))


async def _run(worker: Worker) -> int:
    loop = asyncio.get_running_loop()
    stop_requested = asyncio.Event()
    for sig in _SIGNALS:
        loop.add_signal_handler(sig, _on_signal, sig, stop_requested)
    try:
        try:
            await worker.start()
        except Exception as exc:
            return _startup_failed(exc)
        await stop_requested.wait()
        await worker.stop()
        return 0
    finally:
        for sig in _SIGNALS:
            loop.remove_signal_handler(sig)


def _on_signal(sig: signal.Signals, stop_requested: asyncio.Event) -> None:
    # The event loop calls this (loop.add_signal_handler), outside the signal handler
    # itself. It only asks for the stop; _run calls Worker.stop.
    _log.info("worker.signal", signal=sig.name)
    stop_requested.set()


def _startup_failed(exc: Exception) -> int:
    _log.error("worker.startup_failed", error=_error(exc))
    return EXIT_STARTUP_FAILED
