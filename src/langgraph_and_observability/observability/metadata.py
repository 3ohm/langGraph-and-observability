"""The trace metadata of each run (spec §12.1; P14).

The worker passes ``run_metadata`` to the run executor, which puts it in the graph
config's ``metadata``. langchain-core copies that onto every run of the trace. The
metadata names the run, its agent, and the environment and version that ran it. It
never holds a secret, a DSN or the run's input. An agent adds its own keys, from the
run's input, with ``AgentSpec.trace_metadata``; the executor merges them under these
keys, so a core key wins.

The run is anything with the three attributes of ``TracedRun``, such as the worker's
``RunRow``. This module imports nothing from the worker, which a fork may delete
(spec §13.9).
"""

from typing import Protocol
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


def run_metadata(run: TracedRun, settings: Settings) -> dict[str, str]:
    """Return the core trace metadata of ``run``. Every value is a string.

    The keys are ``run_id``, ``session.id``, ``dedup_key``, ``agent``, ``env``,
    ``app_version`` and ``git_sha``. ``session.id`` holds the run's ID again, which is
    also the graph's ``thread_id``, under its OpenTelemetry name (spec §12.2).
    """
    run_id = str(run.run_id)
    return {
        "run_id": run_id,
        "session.id": run_id,
        "dedup_key": run.dedup_key,
        "agent": run.agent,
        "env": settings.env,
        "app_version": __version__,
        "git_sha": settings.git_sha,
    }
