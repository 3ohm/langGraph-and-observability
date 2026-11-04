"""The Incident Triage agent's data types (spec §9.1, §9.2, §9.5 and §9.8).

Every model forbids unknown fields. Times are stored timezone-aware, in UTC; a time
without a zone is taken to be UTC.

Sizes are capped (spec §8.5). An evidence item's text is at most ``MAX_EVIDENCE_TEXT``
characters: the steps that collect evidence cut text to that length before they build
an item, so a longer text fails validation. ``TriageAnalysis`` caps its summary and its
lists (spec §9.8).

``SERDE_TYPES`` lists the models that the graph keeps in its state, so the checkpoint
serializer revives them (``persistence.serde.make_serde``).
"""

from datetime import UTC, datetime
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

# The longest text of one evidence item, in characters.
MAX_EVIDENCE_TEXT = 4000
# The caps of TriageAnalysis (spec §9.8). The spec gives the first two; the list caps
# keep the model's answer small.
MAX_SUMMARY_CHARS = 600
MAX_RECOMMENDED_ACTIONS = 5
MAX_CITED_EVIDENCE = 20
MAX_SUSPECTED_CODE = 10

# An evidence ID is <source>:<type>:<key>, for example k8s:event:3, log:previous:api,
# dd:event:<id>, code:snippet:2 or note:target:not_found (spec §9.2).
EVIDENCE_ID_PATTERN = r"^(k8s|log|dd|code|note):[a-z_]+:[A-Za-z0-9._/-]+$"


def _as_utc(value: datetime) -> datetime:
    """Return ``value`` in UTC. A naive ``value`` is taken to be UTC already."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


# A datetime, stored timezone-aware in UTC.
UtcDatetime = Annotated[datetime, AfterValidator(_as_utc)]


class _Model(BaseModel):
    """The base of every model here: an unknown field is an error."""

    model_config = ConfigDict(extra="forbid")


class WorkloadRef(_Model):
    """A Kubernetes workload, by kind and name."""

    kind: Literal["Deployment", "StatefulSet", "DaemonSet", "Job"]
    name: str = Field(min_length=1)


class IncidentMessage(_Model):
    """The queue message that starts a triage run (spec §9.1).

    One of ``pod`` or ``workload`` is required. Without ``fired_at``, the time the
    message was received stands in for it. ``raw`` is the producer's original payload,
    kept for reference. A name that is given is never empty: an empty one fails
    validation at intake, so the message goes to the dead-letter queue.
    """

    version: Literal[1]
    source: str
    alert_id: str | None = None
    fired_at: UtcDatetime | None = None
    cluster: str | None = None
    namespace: str = Field(min_length=1)
    pod: str | None = Field(default=None, min_length=1)
    workload: WorkloadRef | None = None
    container: str | None = Field(default=None, min_length=1)
    raw: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _require_pod_or_workload(self) -> Self:
        if self.pod is None and self.workload is None:
            raise ValueError("one of pod or workload is required")
        return self

    def check_cluster(self, expected: str) -> None:
        """Raise ValueError if the message names a cluster other than ``expected``.

        ``expected`` is the cluster the worker runs in (``APP_CLUSTER_NAME``). A message
        that names no cluster passes.
        """
        if self.cluster is not None and self.cluster != expected:
            raise ValueError(
                f"the message is for cluster {self.cluster!r}, "
                f"but this worker serves cluster {expected!r}"
            )


class ImageRef(_Model):
    """The image of one container, and its digest when the pod status reports one."""

    container: str
    image: str
    digest: str | None


class Target(_Model):
    """What the run triages: the message's target, resolved in the cluster."""

    cluster: str
    namespace: str
    workload: WorkloadRef | None
    pod: str | None
    container: str | None
    node: str | None
    images: list[ImageRef]


class EvidenceItem(_Model):
    """One piece of evidence, with a stable ID that the analysis cites (spec §9.2)."""

    id: Annotated[str, Field(pattern=EVIDENCE_ID_PATTERN)]
    kind: Literal["k8s_status", "k8s_event", "log", "node", "datadog_event", "note"]
    title: str
    text: Annotated[str, Field(max_length=MAX_EVIDENCE_TEXT)]
    meta: dict[str, str] = Field(default_factory=dict)


class Frame(_Model):
    """One stack frame parsed from the logs. ``family`` names the language's parser."""

    file: str
    line: int
    function: str | None
    is_app_frame: bool
    family: str


class CodeSnippet(_Model):
    """Source code at the deployed commit: lines ``start_line`` to ``end_line`` of ``path``."""

    id: str
    repo: str
    commit: str
    path: str
    start_line: int
    end_line: int
    symbol: str | None
    language: str
    text: str


class Assessment(_Model):
    """The decision whether to collect code evidence, and its reasons (spec §9.5)."""

    collect_code: bool
    infra_signals: list[str]
    app_signals: list[str]
    reasons: list[str]


# The model's output schema (spec §9.7, §9.8). The analyze step sends the JSON schema
# of TriageAnalysis to the model, so these docstrings are written for the model: pydantic
# puts each one in the schema as the object's description. Every field is required.


class EvidenceCitation(_Model):
    """An evidence item that supports the analysis: its ID, and why it matters."""

    id: str
    why: str


class CodeCitation(_Model):
    """A suspected code location: lines in one of the provided code snippets."""

    snippet_id: str
    path: str
    start_line: int
    end_line: int
    symbol: str | None


class TriageAnalysis(_Model):
    """The triage of one incident.

    Cite at least one evidence item, by its ID, unless the category is unknown.
    """

    category: Literal["infrastructure", "configuration", "application", "dependency", "unknown"]
    summary: Annotated[str, Field(max_length=MAX_SUMMARY_CHARS)]
    root_cause: str
    confidence: Literal["low", "medium", "high"]
    evidence: Annotated[list[EvidenceCitation], Field(max_length=MAX_CITED_EVIDENCE)]
    suspected_code: Annotated[list[CodeCitation], Field(max_length=MAX_SUSPECTED_CODE)]
    recommended_actions: Annotated[list[str], Field(max_length=MAX_RECOMMENDED_ACTIONS)]
    needs_human: bool

    @model_validator(mode="after")
    def _require_evidence(self) -> Self:
        if not self.evidence and self.category != "unknown":
            raise ValueError("evidence must cite at least one item unless the category is unknown")
        return self


class Usage(_Model):
    """The model call's token usage: input, output and cache reads."""

    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int = 0


class TriageResult(_Model):
    """What a run stores in ``triage_results`` and sends to the sinks (spec §9.8, §9.9).

    ``needs_human`` is True when there is no analysis (a refusal, spec §9.7), and else
    the analysis's ``needs_human``. It is a plain field, so it is in the stored JSON and
    in every sink's payload; a value that disagrees with ``analysis`` is an error.
    """

    run_id: UUID
    dedup_key: str
    target: Target
    assessment: Assessment
    analysis: TriageAnalysis | None
    refusal: bool
    needs_human: bool
    model_id: str | None
    usage: Usage | None
    evidence_counts: dict[str, int]
    created_at: UtcDatetime

    @model_validator(mode="after")
    def _check_needs_human(self) -> Self:
        expected = result_needs_human(self.analysis)
        if self.needs_human != expected:
            raise ValueError(
                f"needs_human must be {expected} "
                + ("with no analysis" if self.analysis is None else "as in the analysis")
            )
        return self


def result_needs_human(analysis: TriageAnalysis | None) -> bool:
    """Whether a result with ``analysis`` needs a human: always without an analysis."""
    return True if analysis is None else analysis.needs_human


# Every model that the graph keeps in its state (state.TriageState), with the models
# nested in them, and Frame.
SERDE_TYPES: tuple[type, ...] = (
    WorkloadRef,
    IncidentMessage,
    ImageRef,
    Target,
    EvidenceItem,
    Frame,
    CodeSnippet,
    Assessment,
    EvidenceCitation,
    CodeCitation,
    TriageAnalysis,
    Usage,
)
