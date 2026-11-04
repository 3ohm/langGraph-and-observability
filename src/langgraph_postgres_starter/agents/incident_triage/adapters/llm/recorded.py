"""The recorded model of ``APP_LLM_MODE=recorded`` (spec §9.7, "Recorded mode").

Replies are files, keyed by incident fixture ID and prompt version:
``recorded/<fixture_id>/<prompt_version>/attempt-<attempt>.json`` under the fixtures
directory. fixtures/FORMAT.md describes them.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

import pydantic
from pydantic import ConfigDict, TypeAdapter, with_config

from langgraph_postgres_starter.agents.incident_triage.adapters.call_log import (
    NO_CALL_LOG,
    CallLog,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmAdapter,
    LlmResponse,
)
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import path_inside
from langgraph_postgres_starter.agents.incident_triage.schemas import Usage


# The shape of a recorded reply. A missing or unknown key is an error.
@with_config(ConfigDict(extra="forbid"))
class _RecordedReply(TypedDict):
    text: str
    stop_reason: str
    model_id: str
    usage: Usage


_RECORDED_REPLY = TypeAdapter(_RecordedReply)


@dataclass(frozen=True)
class RecordedLlm(LlmAdapter):
    """Replies with the recorded responses in ``recorded_dir``. Each call writes ``llm.analyze``."""

    recorded_dir: Path
    calls: CallLog = NO_CALL_LOG

    async def analyze(self, req: AnalysisRequest) -> LlmResponse:
        """Return the reply recorded for the request's fixture, prompt version and attempt.

        Raises ValueError without a fixture ID or for a malformed reply, and
        FileNotFoundError, naming the expected path, when no reply is recorded.
        """
        self.calls.record("llm.analyze")
        if req.fixture_id is None:
            raise ValueError(
                "the recorded model (APP_LLM_MODE=recorded) replies only to fixture "
                "incidents, and this request has no fixture ID (the message's raw.fixture_id)"
            )
        path = path_inside(
            self.recorded_dir, f"{req.fixture_id}/{req.prompt_version}/attempt-{req.attempt}.json"
        )
        return _load_reply(path, req)


def _load_reply(path: Path, req: AnalysisRequest) -> LlmResponse:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        raise FileNotFoundError(
            f"no recorded reply for fixture {req.fixture_id!r}, prompt version "
            f"{req.prompt_version!r}, attempt {req.attempt}: expected the file {path}"
        ) from None
    try:
        reply = _RECORDED_REPLY.validate_json(raw)
    except pydantic.ValidationError as exc:
        raise ValueError(
            f"the recorded reply {path} is malformed (see fixtures/FORMAT.md): {exc}"
        ) from exc
    return LlmResponse(**reply)
