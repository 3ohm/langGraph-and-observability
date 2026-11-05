"""Runs one claimed run's graph and records the outcome (spec §8.2-§8.5; P11, P14).

The worker claims a run, then calls ``RunExecutor.execute`` with the claimed row. The
executor invokes the agent's graph with ``thread_id = str(run_id)``, ``durability="sync"``,
a fresh ``RunControl`` and the run deadline. A run that has a checkpoint resumes from it
with ``None`` input (§8.3), so steps that completed do not run again. The executor then
ends the attempt in ``runs``:

- the graph completes: ``mark_success``;
- the graph drains (``GraphDrained``, after ``request_drain_all``): ``release``, so that
  another pod's periodic check resumes the run (§8.4);
- the graph stops at an interrupt: a failed attempt (``mark_error``). The worker gives no
  resume value, so it raises ``InterruptNotSupportedError``. A retry runs the
  interrupted node again;
- any other exception, including the run deadline: ``mark_error``, which returns the
  run to pending or, after ``max_attempts``, fails it.

Each of these acts only while the pod still holds the run's lease. When it does not,
the executor logs ``run.lease_lost`` and returns the run's current status. A
``CancelledError`` propagates once the run has left ``active_run_ids``; the run keeps
its lease, for the caller to release or to expire.

For the whole invocation, the run's ``run_id``, ``agent`` and ``attempt`` are bound as
``structlog.contextvars`` (spec §12.1), so every log line inside the run carries them:
the executor's, a node's and an adapter's, and a library's through ``logging``.
"""

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from uuid import UUID

import structlog
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.errors import GraphDrained
from langgraph.graph.state import CompiledStateGraph
from langgraph.runtime import RunControl
from pydantic import ValidationError

from langgraph_and_observability.agent_spec import AgentSpec, RunInfo
from langgraph_and_observability.logging import get_logger
from langgraph_and_observability.redaction import redact
from langgraph_and_observability.settings import Settings
from langgraph_and_observability.worker.runs import RunRepo, RunRow, RunStatus

_log = get_logger(__name__)

# The longest error message that is stored in runs.error.
_MAX_ERROR_MESSAGE = 2000

# The statuses that mark_error sets. Any other status means the pod lost the lease.
_MARK_ERROR_STATUSES = frozenset({RunStatus.PENDING, RunStatus.FAILED})

# The key of ainvoke's result that holds a graph's interrupts. LangGraph 1.x made its
# INTERRUPT constant private; tests/unit/test_upstream_shape.py pins this key.
_INTERRUPT_KEY = "__interrupt__"


class InterruptNotSupportedError(RuntimeError):
    """The graph stopped at an interrupt, and this worker has no resume value for it.

    The message names the nodes that wait, never an interrupt's value, which can hold
    data from the state.
    """

    def __init__(self, nodes: Sequence[str]) -> None:
        super().__init__(
            "the graph stopped at an interrupt; this worker gives no resume value, so the "
            "attempt fails and the run gets a retry up to APP_MAX_ATTEMPTS; an interactive "
            "graph runs on langgraph dev or the LangGraph Agent Server; the nodes that wait: "
            + ", ".join(nodes)
        )


class RunExecutor:  # pylint: disable=too-many-instance-attributes  # collaborators, caches
    """Executes claimed runs of ``agents`` for the pod ``owner``.

    ``saver`` is the checkpointer that every agent's graph is compiled with, once.
    ``metadata`` maps a run to the trace metadata of its invocation; secrets and the
    run's identity stay out of ``configurable`` (spec §8.5).
    """

    def __init__(
        self,
        *,
        saver: BaseCheckpointSaver[Any],
        agents: Mapping[str, AgentSpec],
        repo: RunRepo,
        settings: Settings,
        owner: str,
        metadata: Callable[[RunRow], dict[str, str]] | None = None,
    ) -> None:
        self._saver = saver
        self._agents = dict(agents)
        self._graphs: dict[str, CompiledStateGraph[Any, Any, Any, Any]] = {
            name: spec.build_graph().compile(checkpointer=saver) for name, spec in agents.items()
        }
        self._repo = repo
        self._settings = settings
        self._owner = owner
        self._metadata = metadata
        self._controls: dict[UUID, RunControl] = {}

    async def execute(self, run: RunRow) -> RunStatus:
        """Invoke ``run``'s graph, record the outcome and return the run's new status.

        ``run`` is the row that ``RunRepo.claim`` or ``claim_recoverable`` returned to
        this pod. Returns SUCCESS; PENDING after a drain or a failed attempt; FAILED after
        the last attempt; or, when the pod has lost the lease, the run's current status.
        """
        # runs.attempt counts failed attempts; RunInfo.attempt counts invocations from 1.
        attempt = run.attempt + 1
        fields = {"run_id": str(run.run_id), "agent": run.agent, "attempt": attempt}
        # The bound logger names the run in captured logs too, which skip contextvars.
        log = _log.bind(**fields)
        control = RunControl()
        self._controls[run.run_id] = control
        try:
            with structlog.contextvars.bound_contextvars(**fields):
                return await self._execute(run, attempt, control, log)
        finally:
            self._controls.pop(run.run_id, None)

    async def _execute(
        self, run: RunRow, attempt: int, control: RunControl, log: structlog.stdlib.BoundLogger
    ) -> RunStatus:
        try:
            await self._invoke(run, attempt, control, log)
        except GraphDrained as exc:
            log.info("run.drained", reason=exc.reason)
            if await self._repo.release([run.run_id], self._owner) == 1:
                return RunStatus.PENDING
            return await self._lease_lost(run, log)
        except Exception as exc:  # pylint: disable=broad-exception-caught  # the run fails
            return await self._mark_error(run, error_fields(exc), log)
        if await self._repo.mark_success(run.run_id, self._owner):
            log.info("run.success")
            return RunStatus.SUCCESS
        return await self._lease_lost(run, log)

    def request_drain_all(self, reason: str = "shutdown") -> int:
        """Ask every active run to stop after its current step (spec §8.4 step 2).

        Returns how many runs were asked.
        """
        controls = list(self._controls.values())
        for control in controls:
            control.request_drain(reason)
        _log.info("runs.drain_requested", reason=reason, runs=len(controls))
        return len(controls)

    def active_run_ids(self) -> list[UUID]:
        """Return the runs that are in ``execute`` now."""
        return list(self._controls)

    async def _invoke(
        self, run: RunRow, attempt: int, control: RunControl, log: structlog.stdlib.BoundLogger
    ) -> None:
        spec = self._agents.get(run.agent)
        if spec is None:
            raise LookupError(f"no agent named {run.agent!r} is registered")
        model = spec.input_model.model_validate(run.input)
        info = RunInfo(run_id=run.run_id, dedup_key=run.dedup_key, attempt=attempt)
        config: RunnableConfig = {
            "configurable": {"thread_id": str(run.run_id)},
            "metadata": self._metadata(run) if self._metadata is not None else {},
        }
        # A run with a checkpoint resumes from it; a new run starts from its input.
        resumed = await self._saver.aget_tuple(config) is not None
        graph_input = None if resumed else spec.initial_state(model)
        log.info("run.start", resumed=resumed)
        deadline_s = self._settings.run_deadline_seconds
        # Around the whole invocation: a TimeoutError raised in a node is retried by the
        # graph's retry policy, but this deadline cancels the invocation.
        deadline = asyncio.timeout(deadline_s)
        try:
            async with deadline:
                result = await self._graphs[run.agent].ainvoke(
                    graph_input,
                    config,
                    context=spec.make_context(info, model, self._settings),
                    durability="sync",
                    control=control,
                )
        except TimeoutError as exc:
            if deadline.expired():
                raise TimeoutError(f"the run deadline of {deadline_s} s was exceeded") from exc
            raise
        if isinstance(result, Mapping) and _INTERRUPT_KEY in result:
            # An Interrupt holds no node name; the checkpoint names the nodes that wait.
            raise InterruptNotSupportedError(
                (await self._graphs[run.agent].aget_state(config)).next
            )

    async def _mark_error(
        self, run: RunRow, error: dict[str, str], log: structlog.stdlib.BoundLogger
    ) -> RunStatus:
        status = await self._repo.mark_error(
            run.run_id, self._owner, error, self._settings.max_attempts
        )
        # mark_error returns the current status when the pod no longer holds the run.
        # RUNNING or SUCCESS shows that; PENDING or FAILED may also have been set by
        # another pod, which this return value cannot show.
        if status not in _MARK_ERROR_STATUSES:
            log.warning("run.lease_lost", status=status)
            return status
        if status == RunStatus.FAILED:
            log.error("run.failed", error=error)
        else:
            log.warning("run.attempt_failed", error=error)
        return status

    async def _lease_lost(self, run: RunRow, log: structlog.stdlib.BoundLogger) -> RunStatus:
        current = await self._repo.get(run.run_id)
        if current is None:
            raise LookupError(f"run {run.run_id} does not exist")
        log.warning("run.lease_lost", status=current.status)
        return current.status


def error_fields(exc: BaseException) -> dict[str, str]:
    """An exception as stored in ``runs.error`` and logged: its type and its message.

    The message is redacted and cut to ``_MAX_ERROR_MESSAGE`` characters. A pydantic
    ``ValidationError``'s message names where each failure is and why, never the value
    that failed, which is the run's input.
    """
    if isinstance(exc, ValidationError):
        details = validation_details(exc)
        plural = "" if len(details) == 1 else "s"
        message = f"{len(details)} validation error{plural} for {exc.title}: {'; '.join(details)}"
    else:
        message = str(exc)
    return {"type": type(exc).__name__, "message": redact(message)[:_MAX_ERROR_MESSAGE]}


def validation_details(exc: ValidationError) -> list[str]:
    """Each failure of ``exc`` as ``"<loc>: <msg>"``, redacted.

    An item names where the failure is and why, never the value that failed, which can
    hold secrets. The ``runs create`` command prints these items.
    """
    failures = exc.errors(include_url=False, include_context=False, include_input=False)
    return [
        redact(f"{'.'.join(map(str, failure['loc'])) or '(root)'}: {failure['msg']}")
        for failure in failures
    ]
