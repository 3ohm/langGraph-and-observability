"""The ``analyze`` step and its validation (spec §9.7, §9.8; P16, P18, Review Focus 5).

The step runs with the recorded model. ``python-keyerror`` gets its state from the
simulated steps before ``analyze``. The ``_test/...`` replies (``fixtures/recorded/_test``)
answer ``STATE``, a small state built here: they cite its IDs.
"""

import copy
import json
from typing import Any

import langsmith
import pytest
import requests
from langsmith.run_helpers import get_current_run_tree, tracing_context
from langsmith.run_trees import RunTree

from langgraph_and_observability.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmAdapter,
    LlmResponse,
)
from langgraph_and_observability.agents.incident_triage.adapters.llm.recorded import RecordedLlm
from langgraph_and_observability.agents.incident_triage.evidence import make_item
from langgraph_and_observability.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_and_observability.agents.incident_triage.nodes.analyze import (
    AnalysisInvalid,
    analyze,
    validate_analysis,
)
from langgraph_and_observability.agents.incident_triage.schemas import (
    Assessment,
    CodeSnippet,
    Target,
    Usage,
    WorkloadRef,
)
from langgraph_and_observability.agents.incident_triage.state import TriageState
from langgraph_and_observability.agents.incident_triage.tests.pipeline import (
    RecordingLlm,
    make_runtime,
    state_before_analyze,
)
from langgraph_and_observability.policies import is_transient
from langgraph_and_observability.settings import load_settings

RECORDED = load_settings().fixtures_dir / "recorded"
REPAIR = "Your previous reply was invalid: "
LOG_TEXT = "1: Traceback (most recent call last):\n2: ZeroDivisionError: division by zero"
SNIPPET_TEXT = "def unit_price(total: int, quantity: int) -> int:\n    return total // quantity"

STATE: TriageState = {
    "target": Target(
        cluster="local",
        namespace="shop",
        workload=WorkloadRef(kind="Deployment", name="api"),
        pod="api-6c9d8b7f5-x1k2p",
        container="api",
        node="node-1",
        images=[],
    ),
    "assessment": Assessment(
        collect_code=True,
        infra_signals=[],
        app_signals=["stack_trace"],
        reasons=["Application signal stack_trace: the logs hold 2 app frames."],
    ),
    "k8s_evidence": [
        make_item(
            "k8s:status:api",
            "k8s_status",
            "Status of container api in pod api-6c9d8b7f5-x1k2p",
            "container api: state waiting (CrashLoopBackOff), restarts 4, last exit code 1",
        ),
        make_item("log:previous:api", "log", "Log of the previous run of container api", LOG_TEXT),
    ],
    "datadog_evidence": [
        make_item("dd:event:42", "datadog_event", "api is crash looping", "api restarted 4 times"),
    ],
    "notes": [
        make_item(
            "note:code:unmapped_frames",
            "note",
            "Stack frames not found in the source",
            "/app/api/legacy.py:7",
        ),
    ],
    "code_evidence": [
        CodeSnippet(
            id="code:snippet:1",
            repo="fixture://api",
            commit="fixture",
            path="api/pricing.py",
            start_line=10,
            end_line=11,
            symbol="unit_price",
            language="python",
            text=SNIPPET_TEXT,
        ),
        # A window around a line, with no definition: no symbol.
        CodeSnippet(
            id="code:snippet:2",
            repo="fixture://api",
            commit="fixture",
            path="api/main.py",
            start_line=1,
            end_line=8,
            symbol=None,
            language="python",
            text="import pricing\n",
        ),
    ],
}

VALID: dict[str, Any] = {
    "category": "application",
    "summary": "unit_price divides by a quantity of zero, so api crashes.",
    "root_cause": "An order with quantity 0 reaches unit_price, which divides by it.",
    "confidence": "high",
    "evidence": [
        {"id": "log:previous:api", "why": "The ZeroDivisionError."},
        {"id": "k8s:status:api", "why": "The container crash loops with exit code 1."},
        {"id": "dd:event:42", "why": "The monitor saw the restarts."},
        {"id": "note:code:unmapped_frames", "why": "One frame is not in the source."},
        {"id": "code:snippet:1", "why": "The division."},
    ],
    "suspected_code": [
        {
            "snippet_id": "code:snippet:1",
            "path": "api/pricing.py",
            "start_line": 10,
            "end_line": 11,
            "symbol": "unit_price",
        },
        {
            "snippet_id": "code:snippet:2",
            "path": "api/main.py",
            "start_line": 1,
            "end_line": 8,
            "symbol": None,
        },
    ],
    "recommended_actions": ["Reject orders with quantity 0 before pricing them."],
    "needs_human": False,
}


def _valid(**changes: Any) -> str:
    return json.dumps(VALID | changes)


def _recorded_usage(fixture_id: str, attempt: int) -> Usage:
    path = RECORDED / fixture_id / "v1" / f"attempt-{attempt}.json"
    return Usage.model_validate(json.loads(path.read_text())["usage"])


def _sum(*usages: Usage) -> Usage:
    return Usage(
        input_tokens=sum(u.input_tokens for u in usages),
        output_tokens=sum(u.output_tokens for u in usages),
        cache_read_input_tokens=sum(u.cache_read_input_tokens for u in usages),
    )


def _recording() -> RecordingLlm:
    return RecordingLlm(RecordedLlm(RECORDED))


# --- the step ------------------------------------------------------------------------


async def test_valid_recorded_reply():
    runtime = make_runtime("python-keyerror")
    state = await state_before_analyze("python-keyerror", runtime)

    update = await analyze(state, runtime)

    analysis = update["analysis"]
    assert analysis is not None
    assert analysis.category == "application"
    assert (update["refusal"], update["model_id"]) == (False, "anthropic.claude-opus-5-5")
    assert update["usage"] == _recorded_usage("python-keyerror", 0)
    expected = load_fixture("python-keyerror").expected["code"]
    first = analysis.suspected_code[0]
    assert (first.path, first.start_line, first.end_line, first.symbol) == (
        expected["path"],
        expected["start_line"],
        expected["end_line"],
        expected["symbol"],
    )


async def test_bad_citation_repaired():
    llm = _recording()

    update = await analyze(STATE, make_runtime("_test/bad-citation", llm=llm))

    first, repair = llm.requests
    assert (first.attempt, repair.attempt) == (0, 1)
    assert (repair.system, repair.schema) == (first.system, first.schema)
    assert repair.user.startswith(f"{first.user}\n\n{REPAIR}")
    errors = repair.user.removeprefix(f"{first.user}\n\n{REPAIR}")
    assert "evidence[1].id 'k8s:event:7'" in errors
    assert "suspected_code[0]" in errors and "end_line" in errors
    analysis = update["analysis"]
    assert analysis is not None
    assert analysis.evidence[1].id == "k8s:status:api"
    assert update["refusal"] is False
    assert update["usage"] == _sum(
        _recorded_usage("_test/bad-citation", 0), _recorded_usage("_test/bad-citation", 1)
    )


async def test_invalid_twice_raises():
    llm = _recording()

    with pytest.raises(AnalysisInvalid) as exc_info:
        await analyze(STATE, make_runtime("_test/invalid-json", llm=llm))

    assert [req.attempt for req in llm.requests] == [0, 1]
    assert "not valid JSON" in llm.requests[1].user
    # The error lists what was wrong with the second reply.
    errors = exc_info.value.errors
    assert errors and all("not valid JSON" in error for error in errors)
    assert str(exc_info.value).startswith("the model's analysis is invalid: ")
    # Not transient: the graph's retry policy does not run the step again.
    assert is_transient(exc_info.value) is False


async def test_refusal_marks_needs_human():
    llm = _recording()

    update = await analyze(STATE, make_runtime("_test/refusal", llm=llm))

    # No repair after a refusal: attempt-1.json holds a valid reply that is never read.
    assert [req.attempt for req in llm.requests] == [0]
    assert update == {
        "analysis": None,
        "refusal": True,
        "usage": _recorded_usage("_test/refusal", 0),
        "model_id": "anthropic.claude-opus-5-5",
    }


async def test_max_tokens_stop_triggers_repair():  # Review Focus 5
    llm = _recording()

    update = await analyze(STATE, make_runtime("_test/max-tokens", llm=llm))

    first, repair = llm.requests
    errors = repair.user.removeprefix(f"{first.user}\n\n{REPAIR}")
    # The stop reason is the error, not the truncated JSON.
    assert "max_tokens" in errors
    assert "JSON" not in errors
    assert update["analysis"] is not None
    assert update["usage"] == _sum(
        _recorded_usage("_test/max-tokens", 0), _recorded_usage("_test/max-tokens", 1)
    )


async def test_max_tokens_stop_fails_even_when_the_text_parses():  # Review Focus 5
    class CutOff:
        async def analyze(self, req: AnalysisRequest) -> LlmResponse:
            return LlmResponse(
                text=_valid(),
                stop_reason="max_tokens",
                model_id="anthropic.claude-opus-5-5",
                usage=Usage(input_tokens=5, output_tokens=16000),
            )

    llm = RecordingLlm(CutOff())

    with pytest.raises(AnalysisInvalid, match="max_tokens"):
        await analyze(STATE, make_runtime("_test/max-tokens", llm=llm))

    assert len(llm.requests) == 2


class NoNetworkSession(requests.Session):
    """A requests session that fails every request."""

    def request(self, *args, **kwargs):
        raise AssertionError("a unit test tried to reach the network")


async def test_one_llm_run_per_attempt():  # ruling R18
    runs: list[RunTree] = []

    class Spy:
        def __init__(self, inner: LlmAdapter) -> None:
            self.inner = inner

        async def analyze(self, req: AnalysisRequest) -> LlmResponse:
            run = get_current_run_tree()
            assert run is not None
            runs.append(run)
            return await self.inner.analyze(req)

    # "local" builds run trees and sends nothing; the client cannot send either.
    client = langsmith.Client(
        api_url="http://127.0.0.1:1",
        api_key="test-key",
        session=NoNetworkSession(),
        auto_batch_tracing=False,
    )
    try:
        with tracing_context(enabled="local", client=client):
            await analyze(STATE, make_runtime("_test/bad-citation", llm=Spy(RecordedLlm(RECORDED))))
    finally:
        client.close(timeout=0)

    assert [(run.name, run.run_type) for run in runs] == [("bedrock.analyze", "llm")] * 2
    # The run's input is the request alone; the adapter is not traced.
    assert [set(run.inputs) for run in runs] == [{"req"}, {"req"}]
    assert [run.metadata["model_id"] for run in runs] == ["anthropic.claude-opus-5-5"] * 2


# --- validate_analysis ---------------------------------------------------------------


def test_validate_accepts_cited_ids_and_a_snippet_without_symbol():  # N58
    analysis = validate_analysis(_valid(), STATE)

    assert [c.id for c in analysis.evidence] == [c["id"] for c in VALID["evidence"]]
    assert analysis.suspected_code[1].symbol is None


def test_validate_accepts_unknown_without_evidence():
    analysis = validate_analysis(
        _valid(category="unknown", confidence="low", evidence=[], suspected_code=[]), STATE
    )

    assert analysis.category == "unknown"


def _code(index: int, **changes: Any) -> list[dict[str, Any]]:
    code = [dict(c) for c in VALID["suspected_code"]]
    code[index] |= changes
    return code


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"evidence": [{"id": "k8s:event:7", "why": "x"}]}, "evidence[0].id 'k8s:event:7'"),
        # Membership, not the ID pattern (N42): a trailing newline is another ID.
        (
            {"evidence": [{"id": "k8s:status:api\n", "why": "x"}]},
            "evidence[0].id 'k8s:status:api\\n'",
        ),
        ({"suspected_code": _code(0, snippet_id="code:snippet:9")}, "'code:snippet:9'"),
        ({"suspected_code": _code(0, path="api/other.py")}, "suspected_code[0]: path"),
        ({"suspected_code": _code(0, start_line=9)}, "suspected_code[0]: start_line"),
        ({"suspected_code": _code(0, symbol=None)}, "suspected_code[0]: symbol"),
        ({"suspected_code": _code(1, symbol="main")}, "suspected_code[1]: symbol"),
        ({"summary": "x" * 601}, "summary"),
        ({"recommended_actions": ["step"] * 6}, "recommended_actions"),
        ({"evidence": []}, "at least one item"),
        ({"category": "network"}, "category"),
        ({"severity": "high"}, "severity"),
    ],
    ids=[
        "unknown-evidence-id",
        "id-with-newline",
        "unknown-snippet",
        "wrong-path",
        "wrong-start-line",
        "symbol-missing",
        "symbol-on-a-snippet-without-one",
        "summary-too-long",
        "too-many-actions",
        "no-evidence",
        "bad-category",
        "extra-field",
    ],
)
def test_validate_rejects(changes, expected):
    with pytest.raises(AnalysisInvalid) as exc_info:
        validate_analysis(_valid(**changes), STATE)

    assert any(expected in error for error in exc_info.value.errors), exc_info.value.errors


@pytest.mark.parametrize("text", ["", "not json {", '{"category": "application"', "[]"])
def test_validate_rejects_what_is_not_a_json_object(text):
    with pytest.raises(AnalysisInvalid) as exc_info:
        validate_analysis(text, STATE)

    assert exc_info.value.errors


def test_validate_collects_every_error_and_names_only_ids():
    text = _valid(
        summary="secret summary " * 50,
        evidence=[{"id": "k8s:event:7", "why": "x"}, {"id": "dd:event:1", "why": "y"}],
    )

    with pytest.raises(AnalysisInvalid) as exc_info:
        validate_analysis(text, STATE)
    # The schema error comes first; then no citation is checked.
    assert len(exc_info.value.errors) == 1

    text = _valid(
        evidence=[{"id": "k8s:event:7", "why": "x"}, {"id": "dd:event:1", "why": "y"}],
        suspected_code=_code(0, end_line=12, symbol="price"),
    )
    with pytest.raises(AnalysisInvalid) as exc_info:
        validate_analysis(text, STATE)

    errors = exc_info.value.errors
    assert len(errors) == 3
    joined = " ".join(errors)
    # The errors name IDs and fields, never the evidence or the code.
    for text_of_evidence in (SNIPPET_TEXT, "ZeroDivisionError", "unit_price", "api/pricing.py"):
        assert text_of_evidence not in joined


def test_validate_errors_do_not_repeat_the_reply():
    with pytest.raises(AnalysisInvalid) as exc_info:
        validate_analysis(_valid(summary="password=hunter2 " * 40), STATE)

    assert "hunter2" not in " ".join(exc_info.value.errors)


def test_analysis_invalid_keeps_its_errors_when_copied():
    # A copy (or a pickle) rebuilds an exception from its args.
    error = copy.deepcopy(AnalysisInvalid(["a", "b"]))

    assert error.errors == ["a", "b"]
    assert str(error) == "the model's analysis is invalid: a; b"
