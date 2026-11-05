"""The trace metadata of each run (spec §12.1; P14).

The worker passes ``run_metadata`` to the run executor, which puts it in the graph
config's ``metadata``. langchain-core copies that onto every run of the trace. The
metadata names the run and where it ran. It never holds a secret, a DSN or the run's
input; only ``namespace`` and ``workload`` are read from the input.

The run is anything with the four attributes of ``TracedRun``, such as the worker's
``RunRow``. This module imports nothing from the worker, which a fork may delete
(spec §13.9).
"""

from collections.abc import Mapping
from typing import Any, Protocol
from uuid import UUID

from langgraph_and_observability import __version__
from langgraph_and_observability.settings import Settings


class TracedRun(Protocol):
    """What ``run_metadata`` reads from a run."""

    @property
    def run_id(self) -> UUID:
        """The run's ID."""

    @property
    def dedup_key(self) -> str:
        """The run's dedup key."""

    @property
    def agent(self) -> str:
        """The name of the run's agent."""

    @property
    def input(self) -> Mapping[str, Any]:
        """The run's input."""


def run_metadata(run: TracedRun, settings: Settings) -> dict[str, str]:
    """Return the trace metadata of ``run``. Every value is a string.

    The keys are ``run_id``, ``dedup_key``, ``agent``, ``cluster``, ``env``,
    ``app_version`` and ``git_sha``. ``namespace`` is added when the input has one, and
    ``workload`` when the input has one, as ``"<kind>/<name>"`` (``"Deployment/checkout"``).
    """
    metadata = {
        "run_id": str(run.run_id),
        "dedup_key": run.dedup_key,
        "agent": run.agent,
        "cluster": settings.cluster_name,
        "env": settings.env,
        "app_version": __version__,
        "git_sha": settings.git_sha,
    }
    namespace = run.input.get("namespace")
    if isinstance(namespace, str):
        metadata["namespace"] = namespace
    workload = _workload(run.input.get("workload"))
    if workload is not None:
        metadata["workload"] = workload
    return metadata


def _workload(value: object) -> str | None:
    """``{"kind": ..., "name": ...}`` as ``"<kind>/<name>"``, a string as it is, else None."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        kind, name = value.get("kind"), value.get("name")
        if isinstance(kind, str) and isinstance(name, str):
            return f"{kind}/{name}"
    return None
