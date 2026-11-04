"""The trace metadata of each run (spec §12.1; P14).

The worker passes ``run_metadata`` to the run executor, which puts it in the graph
config's ``metadata``. langchain-core copies that onto every run of the trace. The
metadata names the run and where it ran. It never holds a secret, a DSN or the run's
input; only ``namespace`` and ``workload`` are read from the input.
"""

from langgraph_postgres_starter import __version__
from langgraph_postgres_starter.settings import Settings
from langgraph_postgres_starter.worker.runs import RunRow


def run_metadata(run: RunRow, settings: Settings) -> dict[str, str]:
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
