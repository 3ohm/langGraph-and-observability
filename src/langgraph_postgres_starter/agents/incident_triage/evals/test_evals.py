"""The Incident Triage evals (spec §12.3; P21): every case through the graph, then scored.

Each case (``dataset.load_cases``) runs the compiled graph as the worker runs it, with an
in-memory checkpointer, the simulated adapters and the model of ``APP_LLM_MODE``. The
evaluators score each run, and the test fails when an aggregate misses its threshold
(``evaluators.THRESHOLDS``):

- ``recorded``, the default: the recorded replies, with no network. Normal CI runs this
  (``make test``). The judge is skipped.
- ``bedrock``: Claude on Bedrock, and the judge (``APP_JUDGE_MODEL_ID``, else the main
  model). The Evals workflow runs this (``.github/workflows/evals.yml``). AWS
  credentials come from boto3's chain.

A run that fails scores 0 on every evaluator that applies to its case, so it fails the
citation threshold; a judge that fails leaves its case unscored.

The root conftest hides every ``APP_*`` and ``LANGSMITH_*`` variable from each test.
This module reads the variables it needs when it is imported, before any test runs. In
bedrock mode, ``eval_env`` gives the model's variables (``MODEL_VARIABLES``) back to the
test, and LangSmith's when the run is tracked; the adapters stay simulated. In recorded
mode the test sees none of them. So ``APP_LLM_MODE=bedrock`` in the shell makes this
test, and only this one, call Bedrock in ``make test`` too.

With ``LANGSMITH_API_KEY`` set in bedrock mode, the test is marked
``@pytest.mark.langsmith``: langsmith's pytest plugin tracks it in an experiment, and
the scores become feedback there (``observability.langsmith.log_eval_results``). The
scorecard (``evaluators.render_scorecard``) is printed, and appended to the file that
``APP_EVAL_SCORECARD`` names, such as the job summary, before the thresholds are
checked.
"""

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import LlmAdapter
from langgraph_postgres_starter.agents.incident_triage.evals.dataset import EvalCase, load_cases
from langgraph_postgres_starter.agents.incident_triage.evals.evaluators import (
    CATEGORY,
    CITATIONS,
    CODE_LOCATION,
    JUDGE,
    EvalResult,
    category_match,
    citations_valid,
    code_location_hit,
    evidence_ids,
    judge_model,
    judge_root_cause,
    missed_thresholds,
    outputs_of,
    render_scorecard,
    scores_by_key,
)
from langgraph_postgres_starter.agents.incident_triage.evidence import cap_text
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_postgres_starter.agents.incident_triage.schemas import IncidentMessage
from langgraph_postgres_starter.agents.incident_triage.spec import spec
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.observability.langsmith import log_eval_results
from langgraph_postgres_starter.persistence.serde import make_serde
from langgraph_postgres_starter.redaction import redact
from langgraph_postgres_starter.settings import Settings, get_settings, load_settings

# The settings of the model and the judge, which a bedrock run takes from its environment.
MODEL_VARIABLES = (
    "APP_LLM_MODE",
    "APP_BEDROCK_MODEL_ID",
    "APP_JUDGE_MODEL_ID",
    "APP_MODEL_EFFORT",
    "APP_MAX_OUTPUT_TOKENS",
    "APP_AWS_REGION",
)
# Read at import: the root conftest hides these variables from each test.
LIVE = os.environ.get("APP_LLM_MODE", "").strip() == "bedrock"
TRACKED = LIVE and bool(os.environ.get("LANGSMITH_API_KEY", "").strip())
SCORECARD = os.environ.get("APP_EVAL_SCORECARD", "").strip() or None
_RESTORED = {
    name: value
    for name, value in os.environ.items()
    if (LIVE and name in MODEL_VARIABLES)
    or (TRACKED and name.upper().startswith(("LANGSMITH_", "LANGCHAIN_")))
}
# The cases that run at once: a bedrock case takes tens of seconds.
CONCURRENCY = 4
# The longest error message in the scorecard, in characters.
_MAX_ERROR_CHARS = 300

# langsmith's pytest plugin registers the marker.
pytestmark: list[pytest.MarkDecorator] = [pytest.mark.langsmith] if TRACKED else []


@pytest.fixture
def eval_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """In bedrock mode, the variables that this module read at import (``_RESTORED``)."""
    for name, value in _RESTORED.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()


# No argument: the plugin would send a tracked test's arguments to LangSmith as inputs.
@pytest.mark.usefixtures("eval_env")
async def test_evals():
    settings = load_settings(adapters="simulated")
    judge = judge_model(settings)
    cases = load_cases()
    limit = asyncio.Semaphore(CONCURRENCY)

    scored = await asyncio.gather(*(_score(case, settings, judge, limit) for case in cases))

    case_results = {case.fixture_id: results for case, results in zip(cases, scored, strict=True)}
    scores = scores_by_key(case_results)
    url = log_eval_results({"scores": scores, "cases": case_results}, scores) if TRACKED else None
    _publish(render_scorecard(case_results, mode=settings.llm_mode, experiment_url=url))
    missed = missed_thresholds(scores)
    assert missed == [], f"scores below their thresholds: {missed} (see the scorecard)"


async def _score(
    case: EvalCase, settings: Settings, judge: LlmAdapter | None, limit: asyncio.Semaphore
) -> list[EvalResult]:
    """Run one case and score it with every evaluator."""
    async with limit:
        try:
            state = await _run(case, settings)
        except Exception as exc:
            return _failed(case, f"the run failed: {_describe(exc)}", judged=judge is not None)
        outputs = outputs_of(state)
        results = [
            category_match(outputs, case.expected),
            code_location_hit(outputs, case.expected),
            citations_valid(outputs, evidence_ids(state)),
        ]
        try:
            results.append(await judge_root_cause(outputs, case.expected, llm=judge))
        except Exception as exc:
            results.append(
                {"key": JUDGE, "score": None, "comment": f"no verdict: {_describe(exc)}"}
            )
        return results


async def _run(case: EvalCase, settings: Settings) -> TriageState:
    """The final state of a new run of the case's incident, as the worker runs it."""
    message = IncidentMessage.model_validate(load_fixture(case.fixture_id).incident)
    info = RunInfo(run_id=new_id(), dedup_key=spec.dedup_key(message, datetime.now(UTC)), attempt=1)
    graph = spec.build_graph().compile(
        checkpointer=InMemorySaver(serde=make_serde(spec.serde_types))
    )
    state = await graph.ainvoke(
        spec.initial_state(message),
        {"configurable": {"thread_id": str(info.run_id)}},
        context=spec.make_context(info, message, settings),
        durability="sync",
    )
    return cast(TriageState, state)


def _failed(case: EvalCase, comment: str, *, judged: bool) -> list[EvalResult]:
    """The results of a case whose run failed: 0 on every evaluator that applies to it."""
    applies = {
        CATEGORY: True,
        CODE_LOCATION: case.expected["code"] is not None,
        CITATIONS: True,
        JUDGE: judged,
    }
    return [
        {"key": key, "score": 0.0 if scored else None, "comment": comment}
        for key, scored in applies.items()
    ]


def _describe(exc: Exception) -> str:
    """An error's type and message, redacted and capped (the scorecard is public)."""
    return cap_text(redact(f"{type(exc).__name__}: {exc}"), _MAX_ERROR_CHARS)


def _publish(scorecard: str) -> None:
    """Print the scorecard, and append it to ``APP_EVAL_SCORECARD``'s file when set."""
    print(scorecard)
    if SCORECARD is not None:
        with Path(SCORECARD).open("a", encoding="utf-8") as summary:
            summary.write(scorecard)
