"""The ``analyze`` step: the model's triage of the evidence, validated (spec §9.7; P16, P18).

1. The request: the fixed system prompt, the evidence as the user message
   (``prompts.render_user_message``) and the answer's schema (``prompts.analysis_schema``).
2. The model's reply is checked (``validate_analysis``): it must be the JSON of a
   ``TriageAnalysis`` within its caps, cite only evidence that the run collected, and
   name only code snippets that the run collected, with their path, lines and symbol.
3. Attempts:

   - ``stop_reason == "refusal"``: the step returns no analysis and ``refusal=True``.
     Publishing marks the result ``needs_human``. No retry.
   - An invalid reply, or ``stop_reason == "max_tokens"`` (a cut-off reply, even one
     that parses; Review Focus 5): one repair attempt, with ``"Your previous reply was
     invalid: <errors>"`` appended to the user message.
   - A second failure raises ``AnalysisInvalid``, which is not transient: the graph's
     retry policy does not run the step again.

``usage`` is the sum over the attempts, and ``model_id`` is that of the last reply.

Tracing (ruling R18): each model call, not the step, is one LangSmith LLM run named
``bedrock.analyze`` (``trace_llm``), in every ``APP_LLM_MODE``. Its input is the
request, its output the reply, whose text holds the category; its metadata has the
model ID and the token usage. The step's own run, which LangGraph traces, outputs the
analysis, and the step logs ``analyze.done`` with the category.
"""

from collections.abc import Sequence

import pydantic
from langgraph.runtime import Runtime
from pydantic_core import ErrorDetails

from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmAdapter,
    LlmResponse,
)
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext, ctx
from langgraph_postgres_starter.agents.incident_triage.evidence import cap_text
from langgraph_postgres_starter.agents.incident_triage.prompts import (
    PROMPT_VERSION,
    SYSTEM_PROMPT,
    analysis_schema,
    render_user_message,
)
from langgraph_postgres_starter.agents.incident_triage.schemas import TriageAnalysis, Usage
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.logging import get_logger
from langgraph_postgres_starter.observability.langsmith import trace_llm
from langgraph_postgres_starter.redaction import redact

_log = get_logger(__name__)

# The first call and its one repair.
MAX_ATTEMPTS = 2
REPAIR_PREFIX = "Your previous reply was invalid: "
# The longest error message, in characters: an error can quote an ID that the model made.
_MAX_ERROR_CHARS = 300
# The fields of a code citation that must equal those of its snippet.
_CODE_FIELDS = ("path", "start_line", "end_line", "symbol")
# The error of a reply that stopped at max_tokens.
_CUT_OFF = "the reply stopped at the max_tokens limit before it was complete; write a shorter one"


class AnalysisInvalid(ValueError):
    """The model's reply is not a valid analysis. ``errors`` says why, one item per problem.

    The errors name fields and evidence IDs, never the evidence text, and are redacted.
    """

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = list(errors)
        # The errors are the one arg, so a copy or a pickle rebuilds the exception.
        super().__init__(self.errors)

    def __str__(self) -> str:
        return "the model's analysis is invalid: " + "; ".join(self.errors)


async def analyze(state: TriageState, runtime: Runtime[TriageContext]) -> TriageState:
    """The graph's ``analyze`` step. Returns ``analysis``, ``refusal``, ``usage`` and ``model_id``.

    The module docstring gives the attempt rules. Raises ``AnalysisInvalid`` when the
    repair attempt fails too; the model adapter's errors are raised as they are.
    """
    context = ctx(runtime, state.get("message"))
    user = render_user_message(state)
    schema = analysis_schema()
    usage = Usage(input_tokens=0, output_tokens=0)
    errors: list[str] = []
    for attempt in range(MAX_ATTEMPTS):
        req = AnalysisRequest(
            system=SYSTEM_PROMPT,
            user=user if not errors else f"{user}\n\n{REPAIR_PREFIX}{'; '.join(errors)}",
            schema=schema,
            fixture_id=context.fixture_id,
            attempt=attempt,
            prompt_version=PROMPT_VERSION,
        )
        response = await _call_model(context.adapters.llm, req)
        usage = _add(usage, response.usage)
        if response.stop_reason == "refusal":
            _log.warning("analyze.refusal", attempt=attempt, model_id=response.model_id)
            return {
                "analysis": None,
                "refusal": True,
                "usage": usage,
                "model_id": response.model_id,
            }
        try:
            analysis = _checked(response, state)
        except AnalysisInvalid as invalid:
            errors = invalid.errors
            _log.warning("analyze.invalid", attempt=attempt, errors=errors)
            continue
        _log.info(
            "analyze.done",
            category=analysis.category,
            confidence=analysis.confidence,
            attempts=attempt + 1,
            model_id=response.model_id,
        )
        return {
            "analysis": analysis,
            "refusal": False,
            "usage": usage,
            "model_id": response.model_id,
        }
    raise AnalysisInvalid(errors)


def validate_analysis(text: str, state: TriageState) -> TriageAnalysis:
    """The analysis in the model's reply ``text``, checked against the run's evidence.

    Raises ``AnalysisInvalid`` with every problem found: ``text`` that is not the JSON
    of a ``TriageAnalysis`` within its caps (then no citation is checked), a cited
    evidence ID that is not the ID of a collected item or code snippet, and a code
    citation that names no collected snippet or whose path, lines or symbol differ from
    that snippet's. A snippet's symbol may be None; the citation's must then be None
    too. IDs are compared as they are, by membership.
    """
    try:
        analysis = TriageAnalysis.model_validate_json(text)
    except pydantic.ValidationError as exc:
        raise AnalysisInvalid([_schema_error(error) for error in exc.errors()]) from None
    items = [
        *state.get("k8s_evidence", []),
        *state.get("datadog_evidence", []),
        *state.get("notes", []),
    ]
    known = {item.id for item in items}
    snippets = {snippet.id: snippet for snippet in state.get("code_evidence", [])}
    known |= snippets.keys()
    errors: list[str] = []
    for index, citation in enumerate(analysis.evidence):
        if citation.id not in known:
            errors.append(
                f"evidence[{index}].id {citation.id!r} is not the ID of a provided evidence item"
            )
    for index, code in enumerate(analysis.suspected_code):
        snippet = snippets.get(code.snippet_id)
        if snippet is None:
            errors.append(
                f"suspected_code[{index}].snippet_id {code.snippet_id!r} is not the ID of a "
                "provided code snippet"
            )
            continue
        differ = [
            field for field in _CODE_FIELDS if getattr(code, field) != getattr(snippet, field)
        ]
        if differ:
            verb = "does" if len(differ) == 1 else "do"
            errors.append(
                f"suspected_code[{index}]: {', '.join(differ)} {verb} not match the title of "
                f"the snippet {code.snippet_id}"
            )
    if errors:
        raise AnalysisInvalid([_bounded(error) for error in errors])
    return analysis


async def _call_model(llm: LlmAdapter, req: AnalysisRequest) -> LlmResponse:
    """One model call, traced as one LLM run whose input is ``req`` alone.

    ``trace_llm`` wraps the adapter's bound method, so the adapter is not a traced input.
    """
    return await trace_llm("bedrock.analyze")(llm.analyze)(req)


def _checked(response: LlmResponse, state: TriageState) -> TriageAnalysis:
    """The reply's analysis. A cut-off reply (``max_tokens``) is invalid even if it parses."""
    if response.stop_reason == "max_tokens":
        raise AnalysisInvalid([_CUT_OFF])
    return validate_analysis(response.text, state)


def _schema_error(error: ErrorDetails) -> str:
    """One pydantic error as a line: where it is and what is wrong, without the input."""
    if error["type"] == "json_invalid":
        return _bounded(f"the reply is not valid JSON: {error['msg']}")
    location = ".".join(str(part) for part in error["loc"])
    return _bounded(f"{location}: {error['msg']}" if location else error["msg"])


def _bounded(error: str) -> str:
    return cap_text(redact(error), _MAX_ERROR_CHARS)


def _add(total: Usage, more: Usage) -> Usage:
    return Usage(
        input_tokens=total.input_tokens + more.input_tokens,
        output_tokens=total.output_tokens + more.output_tokens,
        cache_read_input_tokens=total.cache_read_input_tokens + more.cache_read_input_tokens,
    )
