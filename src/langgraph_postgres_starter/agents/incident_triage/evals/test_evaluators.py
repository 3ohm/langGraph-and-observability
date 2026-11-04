"""The eval dataset, the evaluators, the judge and the scorecard (spec §12.3; P21).

No test here reaches the network: the judge's model is a fake, and in recorded mode no
Bedrock model is built at all.
"""

import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from langgraph_postgres_starter.agents.incident_triage.adapters.llm import bedrock
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.bedrock import (
    BedrockLlm,
    build_request_body,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmResponse,
)
from langgraph_postgres_starter.agents.incident_triage.evals import evaluators
from langgraph_postgres_starter.agents.incident_triage.evals.dataset import EvalCase, load_cases
from langgraph_postgres_starter.agents.incident_triage.evals.evaluators import (
    CATEGORY,
    CITATIONS,
    CODE_LOCATION,
    JUDGE,
    JUDGE_SCHEMA,
    JUDGE_SYSTEM_PROMPT,
    THRESHOLDS,
    EvalResult,
    aggregate,
    category_match,
    citations_valid,
    code_location_hit,
    judge_model,
    judge_root_cause,
    missed_thresholds,
    render_scorecard,
    scores_by_key,
)
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import list_fixtures
from langgraph_postgres_starter.agents.incident_triage.schemas import (
    CodeCitation,
    EvidenceCitation,
    TriageAnalysis,
    Usage,
)
from langgraph_postgres_starter.settings import load_settings

REFERENCE = {
    "category": "application",
    "code": {
        "path": "orders/service.py",
        "start_line": 23,
        "end_line": 25,
        "symbol": "_flatten_customer",
    },
    "facts": ["KeyError: 'customer'", "_flatten_customer", "order without a customer"],
}
INFRA_REFERENCE = {"category": "infrastructure", "code": None, "facts": ["OOMKilled"]}

# A suspected code location: snippet ID, path, first line, last line.
type Location = tuple[str, str, int, int]


def _outputs(
    category: Any = "application",
    *,
    evidence: tuple[str, ...] = ("log:previous:api",),
    code: tuple[Location, ...] = (("code:snippet:1", "orders/service.py", 20, 30),),
) -> dict[str, Any]:
    """The outputs of a run whose analysis has this category, evidence and code."""
    analysis = TriageAnalysis(
        category=category,
        summary="The order worker crashed.",
        root_cause="_flatten_customer reads order['customer'], and an order without a customer "
        "raises KeyError: 'customer'.",
        confidence="high",
        evidence=[EvidenceCitation(id=item, why="It shows the error.") for item in evidence],
        suspected_code=[
            CodeCitation(snippet_id=sid, path=path, start_line=start, end_line=end, symbol=None)
            for sid, path, start, end in code
        ],
        recommended_actions=[],
        needs_human=False,
    )
    return analysis.model_dump(mode="json")


@dataclass
class FakeJudge:
    """Answers every request with ``text`` and keeps the requests."""

    text: str
    stop_reason: str = "end_turn"
    requests: list[AnalysisRequest] = field(default_factory=list)

    async def analyze(self, req: AnalysisRequest) -> LlmResponse:
        self.requests.append(req)
        return LlmResponse(
            text=self.text,
            stop_reason=self.stop_reason,
            model_id="judge-model",
            usage=Usage(input_tokens=1, output_tokens=1),
        )


# --- dataset ---------------------------------------------------------------------------


def test_load_cases_16():
    cases = load_cases()

    assert len(cases) == 16
    assert [case.fixture_id for case in cases] == list_fixtures()
    assert all(isinstance(case, EvalCase) for case in cases)
    for case in cases:
        assert set(case.expected) == {"category", "code", "facts"}, case.fixture_id
        assert case.expected["facts"], case.fixture_id
    # The spec's infrastructure cases (§12.3) have no expected code location.
    infra = {case.fixture_id for case in cases if case.expected["code"] is None}
    assert infra == {
        "infra-config-error",
        "infra-imagepull",
        "infra-oomkilled",
        "infra-probe-failure",
    }


# --- deterministic evaluators ----------------------------------------------------------


def test_category_match():
    assert category_match(_outputs(), REFERENCE) == {
        "key": CATEGORY,
        "score": 1.0,
        "comment": "expected application, got application",
    }
    wrong = category_match(_outputs("dependency"), REFERENCE)
    assert wrong["score"] == 0.0
    assert wrong["comment"] == "expected application, got dependency"
    # A run without an analysis (a refusal) has no category.
    assert category_match({}, REFERENCE)["score"] == 0.0


def test_code_location_overlap():
    def hit(*code: Location) -> EvalResult:
        return code_location_hit(_outputs(code=code), REFERENCE)

    # The expected location is orders/service.py lines 23-25.
    assert hit(("code:snippet:1", "orders/service.py", 20, 30))["score"] == 1.0
    assert hit(("code:snippet:1", "orders/service.py", 25, 40))["score"] == 1.0
    assert hit(("code:snippet:1", "orders/service.py", 1, 23))["score"] == 1.0
    # One overlapping location among others is enough.
    assert (
        hit(
            ("code:snippet:1", "orders/worker.py", 1, 10),
            ("code:snippet:2", "orders/service.py", 24, 24),
        )["score"]
        == 1.0
    )
    # The same path with lines that do not overlap, and other paths, miss.
    assert hit(("code:snippet:1", "orders/service.py", 26, 40))["score"] == 0.0
    assert hit(("code:snippet:1", "orders/service.py", 1, 22))["score"] == 0.0
    assert hit(("code:snippet:1", "service.py", 23, 25))["score"] == 0.0
    missed = hit()
    assert missed["key"] == CODE_LOCATION
    assert missed["score"] == 0.0
    assert "orders/service.py lines 23-25" in missed["comment"]
    assert code_location_hit({}, REFERENCE)["score"] == 0.0


def test_code_location_excludes_cases_without_expected_code():
    result = code_location_hit(_outputs("infrastructure", code=()), INFRA_REFERENCE)

    assert result["key"] == CODE_LOCATION
    assert result["score"] is None
    assert "not an application case" in result["comment"]


def test_citations_valid_rejects_unknown_id():
    collected = {"log:previous:api", "k8s:status:api-7d9", "code:snippet:1"}

    assert citations_valid(_outputs(), collected)["score"] == 1.0
    unknown = citations_valid(_outputs(evidence=("log:previous:api", "k8s:event:99")), collected)
    assert unknown["key"] == CITATIONS
    assert unknown["score"] == 0.0
    assert "'k8s:event:99'" in unknown["comment"]
    # A suspected code location must name a collected snippet too.
    snippet = citations_valid(
        _outputs(code=(("code:snippet:7", "orders/service.py", 23, 25),)), collected
    )
    assert snippet["score"] == 0.0
    assert "'code:snippet:7'" in snippet["comment"]
    # IDs are compared as they are.
    assert citations_valid(_outputs(evidence=("LOG:previous:api",)), collected)["score"] == 0.0
    # With no analysis there is nothing to cite.
    assert citations_valid({}, collected)["score"] == 1.0


# --- the judge -------------------------------------------------------------------------


async def test_judge_skipped_in_recorded_mode(monkeypatch):
    def no_bedrock(*args, **kwargs):
        raise AssertionError("recorded mode built a Bedrock model or client")

    monkeypatch.setattr(evaluators, "BedrockLlm", no_bedrock)
    monkeypatch.setattr(bedrock, "make_bedrock_client", no_bedrock)
    settings = load_settings()
    assert settings.llm_mode == "recorded"

    llm = judge_model(settings)
    result = await judge_root_cause(_outputs(), REFERENCE, llm=llm)

    assert llm is None
    assert result["key"] == JUDGE
    assert result["score"] is None
    assert result["comment"].startswith("skipped")


def test_judge_model_in_bedrock_mode():
    main = judge_model(load_settings(llm_mode="bedrock"))
    own = judge_model(load_settings(llm_mode="bedrock", judge_model_id="judge-model"))

    assert isinstance(main, BedrockLlm) and isinstance(own, BedrockLlm)
    # The judge's model is APP_JUDGE_MODEL_ID, else the main model.
    assert main._settings.bedrock_model_id == "anthropic.claude-opus-5-5"
    assert own._settings.bedrock_model_id == "judge-model"


async def test_judge_scores_the_root_cause_with_one_structured_call():
    judge = FakeJudge(json.dumps({"score": 1, "reason": "It names the missing customer key."}))

    result = await judge_root_cause(_outputs(), REFERENCE, llm=judge)

    assert result == {
        "key": JUDGE,
        "score": 1.0,
        "comment": "It names the missing customer key.",
    }
    [req] = judge.requests
    assert req.system == JUDGE_SYSTEM_PROMPT
    assert req.schema == JUDGE_SCHEMA
    assert req.attempt == 0
    for text in [*REFERENCE["facts"], "order without a customer raises KeyError", "application"]:
        assert text in req.user
    # The Bedrock body of the judge's request: no sampling parameters, no forced tool
    # choice and no prefill (spec §6.4).
    body = build_request_body(req, load_settings(llm_mode="bedrock"))
    assert set(body) == {"anthropic_version", "max_tokens", "system", "messages", "output_config"}
    assert [message["role"] for message in body["messages"]] == ["user"]
    assert body["output_config"]["format"] == {"type": "json_schema", "schema": JUDGE_SCHEMA}


async def test_judge_message_of_a_case_without_code():
    judge = FakeJudge(json.dumps({"score": 1, "reason": "It names the memory limit."}))

    await judge_root_cause(_outputs("infrastructure", code=()), INFRA_REFERENCE, llm=judge)

    [req] = judge.requests
    assert "Code location: none" in req.user
    assert "Category: infrastructure" in req.user


def test_judge_schema_is_a_score_and_a_reason():
    assert JUDGE_SCHEMA["type"] == "object"
    assert JUDGE_SCHEMA["additionalProperties"] is False
    assert set(JUDGE_SCHEMA["required"]) == {"score", "reason"}
    assert JUDGE_SCHEMA["properties"]["score"]["enum"] == [0, 1]
    assert JUDGE_SCHEMA["properties"]["reason"]["type"] == "string"


async def test_judge_scores_a_wrong_root_cause_zero():
    judge = FakeJudge(json.dumps({"score": 0, "reason": "It blames the database."}))

    result = await judge_root_cause(_outputs(), REFERENCE, llm=judge)

    assert result["score"] == 0.0
    assert result["comment"] == "It blames the database."


@pytest.mark.parametrize(
    ("text", "stop_reason", "why"),
    [
        ("not json", "end_turn", "not a valid verdict"),
        (json.dumps({"score": 2, "reason": "x"}), "end_turn", "not a valid verdict"),
        ("", "refusal", "refusal"),
        (json.dumps({"score": 1, "reason": "x"}), "max_tokens", "max_tokens"),
    ],
)
async def test_judge_without_a_verdict_scores_nothing(text, stop_reason, why):
    result = await judge_root_cause(
        _outputs(), REFERENCE, llm=FakeJudge(text, stop_reason=stop_reason)
    )

    assert result["score"] is None
    assert result["comment"].startswith("no verdict")
    assert why in result["comment"]


async def test_judge_gives_a_run_without_an_analysis_zero():
    judge = FakeJudge(json.dumps({"score": 1, "reason": "x"}))

    result = await judge_root_cause({}, REFERENCE, llm=judge)

    assert result["score"] == 0.0
    assert judge.requests == []


# --- aggregates, thresholds and the scorecard ------------------------------------------


def test_thresholds():
    assert THRESHOLDS == {CATEGORY: 0.9, CODE_LOCATION: 0.8, CITATIONS: 1.0}


def test_aggregate_leaves_out_unscored_results():
    results: list[EvalResult] = [
        {"key": CODE_LOCATION, "score": 1.0, "comment": ""},
        {"key": CODE_LOCATION, "score": None, "comment": "not an application case"},
        {"key": CODE_LOCATION, "score": 0.0, "comment": ""},
    ]

    assert aggregate(results) == 0.5
    assert aggregate([results[1]]) is None


def test_missed_thresholds():
    passing = {CATEGORY: 0.9375, CODE_LOCATION: 0.8, CITATIONS: 1.0, JUDGE: 0.1}
    assert missed_thresholds(passing) == []
    assert missed_thresholds({**passing, CATEGORY: 0.875, CITATIONS: 0.9375}) == [
        CATEGORY,
        CITATIONS,
    ]
    # A threshold whose evaluator scored no case is missed.
    assert missed_thresholds({**passing, CODE_LOCATION: None}) == [CODE_LOCATION]


def test_scorecard_lists_scores_thresholds_and_cases():
    cases: dict[str, list[EvalResult]] = {
        "python-keyerror": [
            {"key": CATEGORY, "score": 1.0, "comment": "expected application, got application"},
            {"key": CODE_LOCATION, "score": 0.0, "comment": "no suspected code location"},
            {"key": CITATIONS, "score": 1.0, "comment": "every cited ID was collected"},
            {"key": JUDGE, "score": None, "comment": "skipped: recorded mode"},
        ],
        "infra-oomkilled": [
            {"key": CATEGORY, "score": 1.0, "comment": "expected infrastructure"},
            {"key": CODE_LOCATION, "score": None, "comment": "not an application case"},
            {"key": CITATIONS, "score": 1.0, "comment": "every cited ID was collected"},
            {"key": JUDGE, "score": None, "comment": "skipped: recorded mode"},
        ],
    }
    scores = scores_by_key(cases)
    assert scores == {CATEGORY: 1.0, CODE_LOCATION: 0.0, CITATIONS: 1.0, JUDGE: None}

    card = render_scorecard(cases, mode="bedrock", experiment_url="https://smith.example/p/1")

    lines = card.splitlines()
    assert lines[0] == "## Incident triage evals (bedrock)"
    assert f"| {CATEGORY} | 1.00 (2/2) | ≥ 0.9 | pass |" in lines
    assert f"| {CODE_LOCATION} | 0.00 (0/1) | ≥ 0.8 | **fail** |" in lines
    assert f"| {CITATIONS} | 1.00 (2/2) | ≥ 1.0 | pass |" in lines
    assert f"| {JUDGE} | skipped | none | report only |" in lines
    assert "LangSmith experiment: https://smith.example/p/1" in lines
    # Every case has a row (R65), and each result below 1 is explained.
    assert "| python-keyerror | 1 | 0 | 1 | – |" in lines
    assert "| infra-oomkilled | 1 | – | 1 | – |" in lines
    assert "- python-keyerror, code_location_hit: no suspected code location" in lines


def test_scorecard_gives_a_comment_shared_by_several_results_once():
    failed = "the run failed: EndpointConnectionError: Could not connect"
    cases: dict[str, list[EvalResult]] = {
        "go-nil-map": [
            {"key": CATEGORY, "score": 0.0, "comment": failed},
            {"key": CODE_LOCATION, "score": 0.0, "comment": failed},
            {"key": CITATIONS, "score": 0.0, "comment": failed},
            {"key": JUDGE, "score": 0.0, "comment": failed},
        ]
    }

    card = render_scorecard(cases, mode="bedrock", experiment_url=None)

    assert (
        f"- go-nil-map, {CATEGORY}, {CODE_LOCATION}, {CITATIONS}, {JUDGE}: {failed}"
        in card.splitlines()
    )


def test_scorecard_without_langsmith():
    card = render_scorecard({}, mode="recorded", experiment_url=None)

    assert "LangSmith experiment: none (the run was not tracked in LangSmith)" in card
