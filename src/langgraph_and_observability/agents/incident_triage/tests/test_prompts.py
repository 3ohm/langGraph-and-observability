"""The analysis prompts and the answer's JSON schema (spec §9.7; P16, P23).

The system prompt is one fixed text, the same for every incident, so Bedrock's prompt
cache can reuse it. The user message holds the incident's evidence in sections, each
item tagged with its ID. The schema is ``TriageAnalysis``'s, cut to the JSON-schema
subset of Bedrock's structured outputs.
"""

import json
from collections.abc import Iterator
from typing import Any

from langgraph_and_observability.agents.incident_triage.adapters.llm.bedrock import (
    build_request_body,
)
from langgraph_and_observability.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmResponse,
)
from langgraph_and_observability.agents.incident_triage.nodes.analyze import analyze
from langgraph_and_observability.agents.incident_triage.prompts import (
    PROMPT_VERSION,
    SYSTEM_PROMPT,
    analysis_schema,
    render_user_message,
)
from langgraph_and_observability.agents.incident_triage.schemas import (
    Assessment,
    EvidenceItem,
    Target,
    TriageAnalysis,
    Usage,
)
from langgraph_and_observability.agents.incident_triage.state import TriageState
from langgraph_and_observability.agents.incident_triage.tests.pipeline import (
    RecordingLlm,
    make_runtime,
    state_before_analyze,
)
from langgraph_and_observability.settings import load_settings

# The keywords that Bedrock's structured outputs reject, or accept only in part (P16.md).
UNSUPPORTED = {
    "maxLength",
    "minLength",
    "maxItems",
    "minItems",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
}

TARGET = Target(
    cluster="local",
    namespace="shop",
    workload=None,
    pod="api-1",
    container="api",
    node=None,
    images=[],
)
ASSESSMENT = Assessment(collect_code=False, infra_signals=[], app_signals=[], reasons=["none"])


class Refuses:
    """A model that refuses every request."""

    async def analyze(self, req: AnalysisRequest) -> LlmResponse:
        return LlmResponse(
            text="",
            stop_reason="refusal",
            model_id="anthropic.claude-opus-5-5",
            usage=Usage(input_tokens=10, output_tokens=1),
        )


def test_system_prompt_states_the_rules():
    assert PROMPT_VERSION == "v1"
    for rule in (
        "Cite only evidence IDs from the provided items",
        'Answer "unknown" when the evidence is not enough',
        "Never invent code locations",
    ):
        assert rule in SYSTEM_PROMPT


def test_system_prompt_is_long_enough_to_cache():
    # Bedrock caches a prefix only from 512 tokens on for Claude Opus 5.5 (P16.md). A
    # token is about 4 characters of English text; this keeps a margin.
    assert len(SYSTEM_PROMPT) >= 4 * 600


async def test_system_prompt_identical_across_incidents():  # P23
    requests: list[AnalysisRequest] = []
    for fixture_id in ("python-keyerror", "infra-oomkilled"):
        llm = RecordingLlm(Refuses())
        runtime = make_runtime(fixture_id, llm=llm)
        await analyze(await state_before_analyze(fixture_id, runtime), runtime)
        requests += llm.requests

    first, second = requests
    assert first.user != second.user
    settings = load_settings()
    blocks = [json.dumps(build_request_body(req, settings)["system"]) for req in requests]
    assert blocks[0] == blocks[1]
    assert json.loads(blocks[0]) == [
        {"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}
    ]


async def test_user_message_has_the_sections_and_tagged_items():
    runtime = make_runtime("python-keyerror")
    state = await state_before_analyze("python-keyerror", runtime)

    text = render_user_message(state)

    assert [line for line in text.splitlines() if line.startswith("## ")] == [
        "## Target",
        "## Assessment",
        "## Kubernetes evidence",
        "## Datadog evidence",
        "## Code evidence",
        "## Notes",
    ]
    for item in [*state["k8s_evidence"], *state["datadog_evidence"]]:
        assert f"[{item.id}] {item.title}\n{item.text}" in text
    first, _, last = state["code_evidence"]
    assert (
        f"[code:snippet:1] orders/service.py, lines 23-25, symbol _flatten_customer\n{first.text}"
        in text
    )
    # A snippet without a symbol (a window around the line) says so.
    assert last.symbol is None
    assert f"[code:snippet:3] orders/worker.py, lines 14-34, no symbol\n{last.text}" in text
    assert "namespace: shop" in text
    assert "pod: orders-7d9f8b6c5d-x2kqp" in text
    assert text.endswith("## Notes\n(none)")


def test_user_message_marks_empty_sections_and_lists_notes():
    note = EvidenceItem(id="note:target:not_found", kind="note", title="No pod", text="gone")
    state: TriageState = {"target": TARGET, "assessment": ASSESSMENT, "notes": [note]}

    text = render_user_message(state)

    for heading in ("Kubernetes evidence", "Datadog evidence", "Code evidence"):
        assert f"## {heading}\n(none)" in text
    assert text.endswith("## Notes\n[note:target:not_found] No pod\ngone")


def test_user_message_is_redacted_again():
    # Items are redacted when they are built (evidence.make_item); this one is not.
    leaked = EvidenceItem(
        id="log:previous:api",
        kind="log",
        title="Log of the previous run",
        text="connecting with password=hunter2-hunter2",
    )
    state: TriageState = {"target": TARGET, "assessment": ASSESSMENT, "k8s_evidence": [leaked]}

    text = render_user_message(state)

    assert "hunter2" not in text
    assert "[REDACTED]" in text
    assert "[log:previous:api] Log of the previous run\n" in text


def _keywords(schema: object) -> Iterator[tuple[str, Any]]:
    """Each keyword of ``schema`` and its subschemas, with its value; not property names."""
    if isinstance(schema, dict):
        for key, value in schema.items():
            yield key, value
            if key in ("properties", "$defs"):
                for subschema in value.values():
                    yield from _keywords(subschema)
            else:
                yield from _keywords(value)
    elif isinstance(schema, list):
        for item in schema:
            yield from _keywords(item)


def _objects(schema: object) -> Iterator[dict[str, Any]]:
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            yield schema
        for value in schema.values():
            yield from _objects(value)
    elif isinstance(schema, list):
        for item in schema:
            yield from _objects(item)


def test_analysis_schema_fits_the_structured_output_subset():
    source = TriageAnalysis.model_json_schema()
    # The caps are in the source schema, so the cut is real.
    assert source["properties"]["summary"]["maxLength"] == 600
    assert source["properties"]["recommended_actions"]["maxItems"] == 5

    schema = analysis_schema()

    assert not UNSUPPORTED & {key for key, _ in _keywords(schema)}
    objects = list(_objects(schema))
    assert len(objects) == 3  # TriageAnalysis, EvidenceCitation and CodeCitation
    assert all(obj["additionalProperties"] is False for obj in objects)
    # Only the keywords go: every field stays, and every field stays required.
    assert schema["properties"].keys() == source["properties"].keys()
    assert schema["required"] == source["required"]
    assert schema["$defs"]["CodeCitation"]["properties"]["symbol"] == {
        "anyOf": [{"type": "string"}, {"type": "null"}],
        "title": "Symbol",
    }
    # The source schema is not changed.
    assert TriageAnalysis.model_json_schema() == source
