"""The example's own trace metadata: where the incident happened (spec §12.1; P14).

``AgentSpec.trace_metadata`` names ``trace_metadata``. The run executor merges its keys
under the core keys (``observability.metadata.run_metadata``). It reads only the
``namespace`` and ``workload`` of the run's stored input, and the cluster's name from
the settings: never a secret.
"""

from collections.abc import Mapping
from typing import Any

from langgraph_and_observability.settings import Settings


def trace_metadata(run_input: Mapping[str, Any], settings: Settings) -> dict[str, str]:
    """Return the example's trace metadata. Every value is a string.

    ``cluster`` is ``APP_CLUSTER_NAME``. ``namespace`` is added when the input has one,
    and ``workload`` when the input has one, as ``"<kind>/<name>"``
    (``"Deployment/checkout"``).
    """
    metadata = {"cluster": settings.cluster_name}
    namespace = run_input.get("namespace")
    if isinstance(namespace, str):
        metadata["namespace"] = namespace
    workload = _workload(run_input.get("workload"))
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
