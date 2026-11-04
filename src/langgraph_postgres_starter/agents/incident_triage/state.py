"""The Incident Triage graph's state (spec §8.5 and §9.2).

Each step writes its own keys, except ``notes``: several steps add notes, and LangGraph
appends each step's list to it (``operator.add``). The steps that collect evidence cap
their lists, so large blobs never go into state.
"""

import operator
from typing import Annotated, TypedDict

from langgraph_postgres_starter.agents.incident_triage.schemas import (
    Assessment,
    CodeSnippet,
    EvidenceItem,
    IncidentMessage,
    Target,
    TriageAnalysis,
    Usage,
)


class TriageState(TypedDict, total=False):
    """The graph's state. A key is absent until the step that writes it has run."""

    message: IncidentMessage
    target: Target
    k8s_evidence: list[EvidenceItem]
    log_text: str
    datadog_evidence: list[EvidenceItem]
    assessment: Assessment
    code_evidence: list[CodeSnippet]
    notes: Annotated[list[EvidenceItem], operator.add]
    analysis: TriageAnalysis | None
    refusal: bool
    usage: Usage | None
    model_id: str | None
    published: list[str]
