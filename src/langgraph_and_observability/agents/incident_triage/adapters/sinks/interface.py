"""The result sink interface (spec §9.9).

The publish step sends each result to every sink in ``APP_RESULT_SINKS``. Each send is
made once per run through the action ledger, with the key ``publish:<run_id>:<name>``,
which the sink gets as ``idempotency_key``.
"""

from typing import Any, Protocol

from langgraph_and_observability.agents.incident_triage.schemas import TriageResult


class ResultSink(Protocol):
    """A destination for triage results."""

    @property
    def name(self) -> str:
        """The sink's name in ``APP_RESULT_SINKS``, such as ``log``."""
        ...

    async def send(self, result: TriageResult, idempotency_key: str) -> dict[str, Any]:
        """Send ``result``. Return a small JSON-serializable summary of the send."""
        ...
