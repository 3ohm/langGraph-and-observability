"""The model adapter's interface (spec §9.7): one model call per request.

``RecordedLlm`` (``recorded.py``) replies with recorded responses
(``APP_LLM_MODE=recorded``).
"""

from dataclasses import dataclass
from typing import Any, Protocol

from langgraph_and_observability.agents.incident_triage.schemas import Usage


@dataclass(frozen=True)
class AnalysisRequest:
    """One model call: the prompts and the JSON schema of the answer.

    ``fixture_id``, ``prompt_version`` and ``attempt`` select the recorded reply in
    recorded mode. ``attempt`` numbers the calls of one analysis, so the repair retry
    gets its own reply. ``fixture_id`` is None outside simulated runs.
    """

    system: str
    user: str
    schema: dict[str, Any]
    fixture_id: str | None
    attempt: int
    prompt_version: str


@dataclass(frozen=True)
class LlmResponse:
    """The model's reply: its text, why it stopped, the model that answered and the tokens.

    ``stop_reason`` is the model's own value, such as ``end_turn``, ``max_tokens`` or
    ``refusal``.
    """

    text: str
    stop_reason: str
    model_id: str
    usage: Usage


class LlmAdapter(Protocol):
    """Sends an analysis request to the model."""

    async def analyze(self, req: AnalysisRequest) -> LlmResponse:
        """The model's reply to ``req``."""
        ...
