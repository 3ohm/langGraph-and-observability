"""The Incident Triage evals (spec §12.3; P21): every case through the graph, then scored.

Each case (``dataset.load_cases``) runs the compiled graph as the worker runs it, with an
in-memory checkpointer, the simulated adapters and the model of ``APP_LLM_MODE``. The
evaluators score each run, and ``test_evals`` fails when an aggregate misses its
threshold (``evaluators.THRESHOLDS``):

- ``recorded``, the default: the recorded replies, with no network. Normal CI runs this
  (``make test``). The judge is skipped.
- ``bedrock``: Claude on Bedrock, and the judge (``APP_JUDGE_MODEL_ID``, else the main
  model). The Evals workflow runs this (``.github/workflows/evals.yml``). AWS
  credentials come from boto3's chain.

A run that fails scores 0 on every evaluator that applies to its case
(``evaluators.run_failed``), so it fails the citation threshold. A judge that fails
leaves its case unscored, and the scorecard lists it under "Not scored".

The root conftest hides every ``APP_*`` and ``LANGSMITH_*`` variable from each test.
This module reads the variables it needs when it is imported, before any test runs. In
bedrock mode, ``eval_env`` gives the model's variables (``MODEL_VARIABLES``) back to
``test_evals``, and LangSmith's when the run is tracked (``_restored_env``); the
adapters stay simulated. In recorded mode the test sees none of them. So
``APP_LLM_MODE=bedrock`` in the shell makes ``test_evals``, and only it, call Bedrock in
``make test`` too.

With ``LANGSMITH_API_KEY`` set in bedrock mode, the run is tracked: ``test_evals``, and
only it, is marked ``@pytest.mark.langsmith``, and ``LANGSMITH_TRACING`` is ``true`` for
it, as langsmith's pytest plugin posts the test run only with tracing on. The plugin
tracks the test in an experiment, and the scores become feedback there
(``observability.langsmith.log_eval_results``). The scorecard
(``evaluators.render_scorecard``) is printed, and appended to the file that
``APP_EVAL_SCORECARD`` names, such as the job summary, before the thresholds are
checked. A LangSmith error is logged and leaves the scorecard without its link; it
never stops the scorecard.
"""

import asyncio
import json
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from langgraph_and_observability.agent_spec import RunInfo
from langgraph_and_observability.agents.incident_triage.adapters.llm.interface import LlmAdapter
from langgraph_and_observability.agents.incident_triage.evals.dataset import EvalCase, load_cases
from langgraph_and_observability.agents.incident_triage.evals.evaluators import (
    CATEGORY,
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
    run_failed,
    scores_by_key,
)
from langgraph_and_observability.agents.incident_triage.evidence import cap_text
from langgraph_and_observability.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_and_observability.agents.incident_triage.schemas import IncidentMessage
from langgraph_and_observability.agents.incident_triage.spec import spec
from langgraph_and_observability.agents.incident_triage.state import TriageState
from langgraph_and_observability.ids import new_id
from langgraph_and_observability.logging import configure_logging, get_logger
from langgraph_and_observability.observability.langsmith import log_eval_results
from langgraph_and_observability.persistence.serde import make_serde
from langgraph_and_observability.redaction import redact
from langgraph_and_observability.settings import Settings, get_settings, load_settings

# The settings of the model and the judge, which a bedrock run takes from its environment.
MODEL_VARIABLES = (
    "APP_LLM_MODE",
    "APP_BEDROCK_MODEL_ID",
    "APP_JUDGE_MODEL_ID",
    "APP_MODEL_EFFORT",
    "APP_MAX_OUTPUT_TOKENS",
    "APP_AWS_REGION",
)
# The cases that run at once: a bedrock case takes tens of seconds.
CONCURRENCY = 4
# The longest error message in the scorecard, in characters.
_MAX_ERROR_CHARS = 300

_log = get_logger(__name__)

# What records a tracked run in LangSmith and returns the experiment's link.
type LogResults = Callable[[dict[str, Any], Mapping[str, float | None]], str | None]


def _is_live(environ: Mapping[str, str]) -> bool:
    """Whether the run uses Bedrock: ``APP_LLM_MODE=bedrock``."""
    return environ.get("APP_LLM_MODE", "").strip() == "bedrock"


def _is_tracked(environ: Mapping[str, str]) -> bool:
    """Whether langsmith's plugin tracks the run: bedrock mode and ``LANGSMITH_API_KEY``."""
    return _is_live(environ) and bool(environ.get("LANGSMITH_API_KEY", "").strip())


def _restored_env(environ: Mapping[str, str]) -> dict[str, str]:
    """The variables of ``environ`` that ``eval_env`` gives back to ``test_evals``.

    In bedrock mode, the model's (``MODEL_VARIABLES``). In a tracked run, also every
    ``LANGSMITH_*`` and ``LANGCHAIN_*`` variable, with ``LANGSMITH_TRACING=true``:
    langsmith's plugin posts the test run, and so the experiment's outputs, feedback and
    link, only when tracing is on. In recorded mode, none.
    """
    live, tracked = _is_live(environ), _is_tracked(environ)
    restored = {
        name: value
        for name, value in environ.items()
        if (live and name in MODEL_VARIABLES)
        or (tracked and name.upper().startswith(("LANGSMITH_", "LANGCHAIN_")))
    }
    if tracked:
        restored["LANGSMITH_TRACING"] = "true"
    return restored


def _track_in_langsmith[F: Callable[..., Any]](tracked: bool) -> Callable[[F], F]:
    """Mark the test ``@pytest.mark.langsmith`` (the plugin registers it) when ``tracked``."""

    def mark(test: F) -> F:
        return pytest.mark.langsmith(test) if tracked else test

    return mark


# Read at import: the root conftest hides these variables from each test.
TRACKED = _is_tracked(os.environ)
SCORECARD = os.environ.get("APP_EVAL_SCORECARD", "").strip() or None
_RESTORED = _restored_env(os.environ)


@pytest.fixture
def eval_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The variables that this module read at import for the run (``_restored_env``)."""
    for name, value in _RESTORED.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()


# No argument: the plugin would send a tracked test's arguments to LangSmith as inputs.
@_track_in_langsmith(TRACKED)
@pytest.mark.usefixtures("eval_env")
async def test_evals():
    settings = load_settings(adapters="simulated")
    judge = judge_model(settings)
    cases = load_cases()
    limit = asyncio.Semaphore(CONCURRENCY)

    scored = await asyncio.gather(*(_score(case, settings, judge, limit) for case in cases))

    case_results = {case.fixture_id: results for case, results in zip(cases, scored, strict=True)}
    scores = _report(
        case_results,
        mode=settings.llm_mode,
        tracked=TRACKED,
        scorecard=None if SCORECARD is None else Path(SCORECARD),
    )
    missed = missed_thresholds(scores)
    assert missed == [], f"scores below their thresholds: {missed} (see the scorecard)"


async def _score(
    case: EvalCase, settings: Settings, judge: LlmAdapter | None, limit: asyncio.Semaphore
) -> list[EvalResult]:
    """Run one case and score it with every evaluator."""
    async with limit:
        try:
            state = await _run(case, settings)
        except Exception as exc:  # pylint: disable=broad-exception-caught  # the case fails
            failed = f"the run failed: {_describe(exc)}"
            return run_failed(case.expected, failed, judged=judge is not None)
        outputs = outputs_of(state)
        results = [
            category_match(outputs, case.expected),
            code_location_hit(outputs, case.expected),
            citations_valid(outputs, evidence_ids(state)),
        ]
        try:
            results.append(await judge_root_cause(outputs, case.expected, llm=judge))
        except Exception as exc:  # pylint: disable=broad-exception-caught  # no verdict
            comment = f"no verdict: the judge failed: {_describe(exc)}"
            results.append({"key": JUDGE, "score": None, "comment": comment})
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


def _report(
    case_results: Mapping[str, Sequence[EvalResult]],
    *,
    mode: str,
    tracked: bool,
    scorecard: Path | None,
    log: LogResults = log_eval_results,
) -> dict[str, float | None]:
    """Record a tracked run in LangSmith, then publish the scorecard; return the scores.

    ``log`` records the run and returns the experiment's link (``log_eval_results``). It
    is called only when ``tracked``. Any error from it is logged as
    ``evals.langsmith_failed``, redacted, and the scorecard has no link. The scorecard is
    printed, and appended to ``scorecard`` when it is set.
    """
    scores = scores_by_key(case_results)
    url = None
    if tracked:
        try:
            url = log({"scores": scores, "cases": case_results}, scores)
        except Exception as exc:  # pylint: disable=broad-exception-caught  # the link is optional
            _log.warning("evals.langsmith_failed", error=_describe(exc))
    card = render_scorecard(case_results, mode=mode, experiment_url=url)
    print(card)
    if scorecard is not None:
        with scorecard.open("a", encoding="utf-8") as summary:
            summary.write(card)
    return scores


def _describe(exc: Exception) -> str:
    """An error's type and message, redacted and capped (the scorecard is public)."""
    return cap_text(redact(f"{type(exc).__name__}: {exc}"), _MAX_ERROR_CHARS)


# --- the harness: the environment, the marker and the report (no network) -------------

SCORED: dict[str, list[EvalResult]] = {
    "python-keyerror": [
        {"key": CATEGORY, "score": 1.0, "comment": "expected application, got application"}
    ]
}


def test_a_tracked_run_turns_langsmith_tracing_on():
    environ = {
        "APP_LLM_MODE": "bedrock",
        "APP_BEDROCK_MODEL_ID": "us.anthropic.claude-opus-5-5",
        "APP_DATABASE_URL": "postgresql://not-for-the-evals",
        "LANGSMITH_API_KEY": "test-key",
        "LANGSMITH_ENDPOINT": "https://eu.api.smith.langchain.com",
        "LANGSMITH_TRACING": "false",
        "PATH": "/usr/bin",
    }

    # langsmith's plugin posts the test run, so its outputs, feedback and link, only
    # with tracing on (langsmith.run_helpers.trace).
    assert _restored_env(environ) == {
        "APP_LLM_MODE": "bedrock",
        "APP_BEDROCK_MODEL_ID": "us.anthropic.claude-opus-5-5",
        "LANGSMITH_API_KEY": "test-key",
        "LANGSMITH_ENDPOINT": "https://eu.api.smith.langchain.com",
        "LANGSMITH_TRACING": "true",
    }


def test_an_untracked_bedrock_run_gets_the_model_variables_only():
    environ = {"APP_LLM_MODE": "bedrock", "LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": " "}

    assert _restored_env(environ) == {"APP_LLM_MODE": "bedrock"}


def test_a_recorded_run_gets_nothing_back():
    environ = {"APP_LLM_MODE": "recorded", "APP_BEDROCK_MODEL_ID": "m", "LANGSMITH_API_KEY": "k"}

    assert _restored_env(environ) == {}
    assert _restored_env({}) == {}


def test_only_a_tracked_run_is_marked_for_langsmith():
    def test_case():
        pass

    marked = _track_in_langsmith(True)(test_case)
    assert [mark.name for mark in getattr(marked, "pytestmark", [])] == ["langsmith"]

    def other_case():
        pass

    assert _track_in_langsmith(False)(other_case) is other_case
    assert not hasattr(other_case, "pytestmark")


def test_a_langsmith_error_still_publishes_the_scorecard(tmp_path, capsys):
    configure_logging(load_settings())
    scorecard = tmp_path / "summary.md"

    def langsmith_down(outputs, scores):
        raise ConnectionError("POST /feedback failed: password=hunter2")

    scores = _report(SCORED, mode="bedrock", tracked=True, scorecard=scorecard, log=langsmith_down)

    assert scores[CATEGORY] == 1.0
    assert "LangSmith experiment: none" in scorecard.read_text()
    out = capsys.readouterr().out
    [line] = [json.loads(text) for text in out.splitlines() if "evals.langsmith_failed" in text]
    assert line["level"] == "warning"
    assert line["error"] == "ConnectionError: POST /feedback failed: password=[REDACTED]"
    assert "hunter2" not in out


def test_a_tracked_report_links_the_experiment(tmp_path):
    logged = []

    def log(outputs, scores):
        logged.append((outputs, scores))
        return "https://smith.example/o/t/projects/p/1"

    scorecard = tmp_path / "summary.md"

    scores = _report(SCORED, mode="bedrock", tracked=True, scorecard=scorecard, log=log)

    assert logged == [({"scores": scores, "cases": SCORED}, scores)]
    text = scorecard.read_text()
    assert "LangSmith experiment: https://smith.example/o/t/projects/p/1" in text


def test_an_untracked_report_does_not_call_langsmith():
    def log(outputs, scores):
        raise AssertionError("an untracked run was logged to LangSmith")

    assert _report(SCORED, mode="recorded", tracked=False, scorecard=None, log=log)[CATEGORY] == 1.0
