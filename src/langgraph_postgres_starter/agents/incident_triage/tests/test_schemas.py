"""The agent's schemas, state and dedup key (spec §9.1, §9.2, §9.8 and §9.11)."""

import hashlib
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, get_args, get_type_hints

import pydantic
import pytest

from langgraph_postgres_starter.agents.incident_triage.dedup import dedup_key
from langgraph_postgres_starter.agents.incident_triage.schemas import (
    MAX_CITED_EVIDENCE,
    MAX_EVIDENCE_TEXT,
    MAX_SUSPECTED_CODE,
    SERDE_TYPES,
    Assessment,
    CodeCitation,
    CodeSnippet,
    EvidenceCitation,
    EvidenceItem,
    Frame,
    ImageRef,
    IncidentMessage,
    Target,
    TriageAnalysis,
    TriageResult,
    Usage,
    WorkloadRef,
)
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.persistence.serde import make_serde

RECEIVED_AT = datetime(2026, 11, 2, 10, 30, tzinfo=UTC)


# One sample of each type in SERDE_TYPES.
WORKLOAD = WorkloadRef(kind="Deployment", name="api")
MESSAGE = IncidentMessage(
    version=1,
    source="datadog-monitor",
    alert_id="dd-123",
    fired_at=datetime(2026, 11, 2, 10, 1, tzinfo=UTC),
    cluster="prod-eu",
    namespace="payments",
    pod="api-7d9f-x2",
    workload=WORKLOAD,
    container="api",
    raw={"monitor": {"id": 123, "tags": ["team:payments"]}, "priority": None},
)
IMAGE = ImageRef(
    container="api", image="registry.example.com/payments/api:1.4.2", digest="sha256:" + "a" * 64
)
TARGET = Target(
    cluster="prod-eu",
    namespace="payments",
    workload=WORKLOAD,
    pod="api-7d9f-x2",
    container="api",
    node="ip-10-0-1-5",
    images=[IMAGE],
)
EVIDENCE = EvidenceItem(
    id="k8s:status:api",
    kind="k8s_status",
    title="Container api",
    text="Last state: Terminated (Error), exit code 1",
    meta={"reason": "Error", "exit_code": "1"},
)
FRAME = Frame(
    file="/app/dist/orders/service.js",
    line=42,
    function="createOrder",
    is_app_frame=True,
    family="javascript",
)
SNIPPET = CodeSnippet(
    id="code:snippet:1",
    repo="example/payments",
    commit="0123456789abcdef0123456789abcdef01234567",
    path="src/orders/service.ts",
    start_line=30,
    end_line=60,
    symbol="createOrder",
    language="typescript",
    text="export function createOrder(order: Order) {\n  return order.customer.id;\n}",
)
ASSESSMENT = Assessment(
    collect_code=True,
    infra_signals=[],
    app_signals=["stack trace"],
    reasons=["A stack trace with no infrastructure signal: collect code."],
)
EVIDENCE_CITATION = EvidenceCitation(id="log:current:api", why="TypeError at service.js:42")
CODE_CITATION = CodeCitation(
    snippet_id="code:snippet:1",
    path="src/orders/service.ts",
    start_line=40,
    end_line=44,
    symbol="createOrder",
)
ANALYSIS = TriageAnalysis(
    category="application",
    summary="createOrder reads a property of undefined.",
    root_cause="The order has no customer, and createOrder does not check for it.",
    confidence="high",
    evidence=[EVIDENCE_CITATION],
    suspected_code=[CODE_CITATION],
    recommended_actions=["Check order.customer before reading its fields."],
    needs_human=False,
)
USAGE = Usage(input_tokens=12000, output_tokens=900, cache_read_input_tokens=8000)

SAMPLES: dict[type, pydantic.BaseModel] = {
    type(sample): sample
    for sample in (
        WORKLOAD,
        MESSAGE,
        IMAGE,
        TARGET,
        EVIDENCE,
        FRAME,
        SNIPPET,
        ASSESSMENT,
        EVIDENCE_CITATION,
        CODE_CITATION,
        ANALYSIS,
        USAGE,
    )
}


def _message(**fields: Any) -> IncidentMessage:
    body = {
        "version": 1,
        "source": "alertmanager",
        "cluster": "prod-eu",
        "namespace": "payments",
        "workload": {"kind": "Deployment", "name": "api"},
    }
    return IncidentMessage.model_validate(body | fields)


def _at(hour: int, minute: int) -> datetime:
    return datetime(2026, 11, 2, hour, minute, tzinfo=UTC)


def _analysis(**fields: Any) -> TriageAnalysis:
    body = {
        "category": "application",
        "summary": "createOrder reads a property of undefined.",
        "root_cause": "The order has no customer, and createOrder does not check for it.",
        "confidence": "medium",
        "evidence": [{"id": "log:current:api", "why": "TypeError at service.js:42"}],
        "suspected_code": [],
        "recommended_actions": ["Check order.customer before reading its fields."],
        "needs_human": False,
    }
    return TriageAnalysis.model_validate(body | fields)


def test_pod_or_workload_required():
    with pytest.raises(pydantic.ValidationError, match="pod or workload"):
        IncidentMessage(version=1, source="manual", namespace="payments")

    assert IncidentMessage(version=1, source="manual", namespace="payments", pod="api-x2").pod
    assert _message().workload == WorkloadRef(kind="Deployment", name="api")


def test_message_rejects_unknown_fields_and_versions():
    with pytest.raises(pydantic.ValidationError, match="severity"):
        _message(severity="high")
    with pytest.raises(pydantic.ValidationError, match="version"):
        _message(version=2)
    with pytest.raises(pydantic.ValidationError, match="kind"):
        _message(workload={"kind": "ReplicaSet", "name": "api-7d9f"})


@pytest.mark.parametrize(
    ("fields", "field"),
    [
        ({"namespace": ""}, "namespace"),
        ({"pod": ""}, "pod"),
        ({"container": ""}, "container"),
        ({"workload": {"kind": "Deployment", "name": ""}}, "name"),
    ],
    ids=["namespace", "pod", "container", "workload-name"],
)
def test_message_rejects_empty_names(fields, field):
    # An empty name fails at intake, so the message goes to the dead-letter queue.
    with pytest.raises(pydantic.ValidationError, match=field):
        _message(**fields)


def test_fired_at_is_stored_as_utc():
    naive = _message(fired_at=datetime(2026, 11, 2, 10, 1))
    other_zone = _message(
        fired_at=datetime(2026, 11, 2, 11, 1, tzinfo=timezone(timedelta(hours=1)))
    )

    assert naive.fired_at == other_zone.fired_at == _at(10, 1)
    assert naive.fired_at is not None and naive.fired_at.tzinfo is UTC
    assert other_zone.fired_at is not None and other_zone.fired_at.tzinfo is UTC


def test_check_cluster_mismatch_raises():
    with pytest.raises(ValueError, match="prod-eu") as exc_info:
        _message().check_cluster("prod-us")
    assert "prod-us" in str(exc_info.value)

    _message().check_cluster("prod-eu")
    _message(cluster=None).check_cluster("prod-us")


def test_dedup_same_15_min_bucket():
    def key(hour: int, minute: int) -> str:
        return dedup_key(_message(fired_at=_at(hour, minute)), RECEIVED_AT)

    assert key(10, 1) == key(10, 14)
    assert key(10, 16) != key(10, 1)
    # Without fired_at, the time the message was received picks the bucket.
    assert dedup_key(_message(), _at(10, 14)) == key(10, 1)
    assert dedup_key(_message(), _at(10, 16)) != key(10, 1)


def test_dedup_key_string():
    # The exact string that is hashed, as the dedup_key docstring shows it.
    def sha256(text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()

    with_workload = _message(fired_at=_at(10, 14))
    with_pod = _message(cluster=None, workload=None, pod="api-7d9f-x2", alert_id="dd-123")

    assert dedup_key(with_workload, RECEIVED_AT) == sha256(
        "prod-eu|payments|Deployment/api|2026-11-02T10:00:00+00:00"
    )
    assert dedup_key(with_pod, RECEIVED_AT) == sha256("|payments|api-7d9f-x2|dd-123")


def test_dedup_uses_alert_id():
    first = _message(alert_id="dd-123", fired_at=_at(10, 1))
    hours_later = _message(alert_id="dd-123", fired_at=_at(13, 40))

    assert dedup_key(first, RECEIVED_AT) == dedup_key(hours_later, RECEIVED_AT)
    assert dedup_key(first, RECEIVED_AT) != dedup_key(
        _message(alert_id="dd-124", fired_at=_at(10, 1)), RECEIVED_AT
    )
    assert dedup_key(first, RECEIVED_AT) != dedup_key(_message(fired_at=_at(10, 1)), RECEIVED_AT)


@pytest.mark.parametrize(
    "evidence_id",
    [
        "k8s:status:api",
        "k8s:event:3",
        "k8s:node:ip-10-0-1-5.eu-west-1.compute.internal",
        "log:previous:api",
        "dd:event:7771234567890",
        "code:snippet:2",
        "note:target:not_found",
        "note:code:unmapped_frames",
    ],
)
def test_evidence_id_accepted(evidence_id):
    assert EvidenceItem(id=evidence_id, kind="note", title="t", text="x", meta={}).id


@pytest.mark.parametrize(
    "evidence_id",
    [
        "k8s:event",
        "k8s:event:",
        "k8s:Event:3",
        "aws:event:3",
        "log:current:a b",
        " k8s:event:3",
        "k8s:event:3\n",
    ],
)
def test_evidence_id_rejected(evidence_id):
    with pytest.raises(pydantic.ValidationError, match="id"):
        EvidenceItem(id=evidence_id, kind="note", title="t", text="x", meta={})


def test_evidence_text_capped():
    assert EvidenceItem(id="log:current:api", kind="log", title="t", text="x" * MAX_EVIDENCE_TEXT)
    with pytest.raises(pydantic.ValidationError, match="text"):
        EvidenceItem(
            id="log:current:api", kind="log", title="t", text="x" * (MAX_EVIDENCE_TEXT + 1)
        )
    with pytest.raises(pydantic.ValidationError, match="kind"):
        EvidenceItem.model_validate(
            {"id": "log:current:api", "kind": "trace", "title": "t", "text": ""}
        )


def test_analysis_caps():
    code = {
        "snippet_id": "code:snippet:1",
        "path": "a.ts",
        "start_line": 1,
        "end_line": 2,
        "symbol": None,
    }
    cited = {"id": "log:current:api", "why": "the stack trace"}

    assert _analysis(summary="x" * 600)
    with pytest.raises(pydantic.ValidationError, match="summary"):
        _analysis(summary="x" * 601)
    with pytest.raises(pydantic.ValidationError, match="recommended_actions"):
        _analysis(recommended_actions=["Roll back."] * 6)
    with pytest.raises(pydantic.ValidationError, match="evidence"):
        _analysis(evidence=[])
    assert _analysis(category="unknown", evidence=[]).evidence == []
    assert _analysis(evidence=[cited] * MAX_CITED_EVIDENCE)
    with pytest.raises(pydantic.ValidationError, match="evidence"):
        _analysis(evidence=[cited] * (MAX_CITED_EVIDENCE + 1))
    assert _analysis(suspected_code=[code] * MAX_SUSPECTED_CODE)
    with pytest.raises(pydantic.ValidationError, match="suspected_code"):
        _analysis(suspected_code=[code] * (MAX_SUSPECTED_CODE + 1))


def test_analysis_json_schema_is_closed():
    # Task 31 sends this schema to the model: every object forbids other properties and
    # lists every property as required.
    schema = TriageAnalysis.model_json_schema()
    objects = [schema, *schema["$defs"].values()]

    assert {d["title"] for d in objects} == {"TriageAnalysis", "EvidenceCitation", "CodeCitation"}
    for obj in objects:
        assert obj["additionalProperties"] is False
        assert set(obj["required"]) == set(obj["properties"])


def _result(**changes: Any) -> TriageResult:
    fields: dict[str, Any] = {
        "run_id": new_id(),
        "dedup_key": "0" * 64,
        "target": TARGET,
        "assessment": ASSESSMENT,
        "analysis": ANALYSIS,
        "refusal": False,
        "needs_human": False,
        "model_id": "anthropic.claude-opus-5-5",
        "usage": USAGE,
        "evidence_counts": {"k8s": 4, "datadog": 1, "code": 1},
        "created_at": datetime(2026, 11, 2, 10, 32),
    }
    return TriageResult(**(fields | changes))


def test_triage_result_json_round_trip():
    result = _result()

    assert result.created_at.tzinfo is UTC
    assert TriageResult.model_validate_json(result.model_dump_json()) == result
    assert result.model_dump(mode="json")["needs_human"] is False


def test_triage_result_needs_human_matches_analysis():
    # Spec §9.7: a result without an analysis (a refusal) needs a human; otherwise
    # needs_human is the analysis's.
    asks = ANALYSIS.model_copy(update={"needs_human": True})
    assert _result(analysis=asks, needs_human=True).needs_human is True
    assert _result(analysis=None, refusal=True, needs_human=True).needs_human is True

    for changes in (
        {"analysis": ANALYSIS, "needs_human": True},
        {"analysis": asks, "needs_human": False},
        {"analysis": None, "refusal": True, "needs_human": False},
    ):
        with pytest.raises(pydantic.ValidationError, match="needs_human"):
            _result(**changes)


@pytest.mark.usefixtures("strict_msgpack")
@pytest.mark.parametrize("type_", SERDE_TYPES, ids=lambda t: t.__name__)
def test_state_types_round_trip_strict_serde(type_):
    value = SAMPLES[type_]  # a KeyError means SAMPLES lacks a sample of this type
    serde = make_serde(SERDE_TYPES)

    loaded = serde.loads_typed(serde.dumps_typed(value))

    assert type(loaded) is type_
    assert loaded == value


@pytest.mark.usefixtures("strict_msgpack")
def test_state_round_trip_strict_serde():
    state: TriageState = {
        "message": MESSAGE,
        "target": TARGET,
        "k8s_evidence": [EVIDENCE],
        "log_text": "TypeError: Cannot read properties of undefined (reading 'id')",
        "datadog_evidence": [],
        "assessment": ASSESSMENT,
        "code_evidence": [SNIPPET],
        "notes": [
            EvidenceItem(id="note:logs:previous_unavailable", kind="note", title="t", text="")
        ],
        "analysis": ANALYSIS,
        "refusal": False,
        "usage": USAGE,
        "model_id": "anthropic.claude-opus-5-5",
        "published": ["log"],
    }
    serde = make_serde(SERDE_TYPES)

    assert serde.loads_typed(serde.dumps_typed(state)) == state


def _models_in(annotation: Any, found: set[type]) -> None:
    """Add every pydantic model that ``annotation`` names, and the models they nest."""
    if isinstance(annotation, type) and issubclass(annotation, pydantic.BaseModel):
        if annotation not in found:
            found.add(annotation)
            for field in annotation.model_fields.values():
                _models_in(field.annotation, found)
        return
    for arg in get_args(annotation):
        _models_in(arg, found)


def test_serde_types_cover_state():
    found: set[type] = set()
    for annotation in get_type_hints(TriageState, include_extras=True).values():
        _models_in(annotation, found)

    assert found <= set(SERDE_TYPES)
    assert len(SERDE_TYPES) == len(set(SERDE_TYPES))
