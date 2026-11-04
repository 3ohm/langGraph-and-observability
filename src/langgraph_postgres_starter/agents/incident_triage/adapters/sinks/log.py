"""The ``log`` sink: each result as one structured log line (spec §9.9). It is the default."""

from dataclasses import dataclass, field
from typing import Any

from langgraph_postgres_starter.agents.incident_triage.adapters.call_log import (
    NO_CALL_LOG,
    CallLog,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.interface import ResultSink
from langgraph_postgres_starter.agents.incident_triage.schemas import TriageResult
from langgraph_postgres_starter.logging import get_logger

_log = get_logger(__name__)


@dataclass(frozen=True)
class LogSink(ResultSink):
    """Writes the log event ``triage.published``, with the result and the idempotency key.

    It is the same in every mode. Each send writes ``sink.log.send`` to ``calls``.
    """

    calls: CallLog = NO_CALL_LOG
    name: str = field(default="log", init=False)

    async def send(self, result: TriageResult, idempotency_key: str) -> dict[str, Any]:
        self.calls.record(f"sink.{self.name}.send")
        _log.info(
            "triage.published",
            result=result.model_dump(mode="json"),
            idempotency_key=idempotency_key,
        )
        return {"sink": self.name}
