"""The assembled Incident Triage graph, its registration and Studio (spec §8.5, §9.2, §10.3, §13.9).

The graph runs with an in-memory checkpointer, the simulated adapters and the recorded
model, as the worker runs it: the agent spec builds the input and the context. Some
tests swap one adapter for a fake that fails, or for a model that answers every
request with one reply. P12: the retry, timeout and error-handler policies; P14: the
context, which holds the settings and their secrets, stays out of traces and
checkpoints.
"""

import asyncio
import dataclasses
import importlib.util
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import langsmith
import pytest
import requests
from langchain_core.runnables import RunnableConfig
from langchain_core.tracers.langchain import LangChainTracer
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph.state import CompiledStateGraph
from pydantic import BaseModel

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.agents.incident_triage import graph as graph_module
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.fake import FakeDatadog
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.interface import DdEvent
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmResponse,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import SourceRef
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext
from langgraph_postgres_starter.agents.incident_triage.dedup import dedup_key
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import (
    FIXTURES_DIR,
    list_fixtures,
    load_fixture,
)
from langgraph_postgres_starter.agents.incident_triage.graph import build_graph
from langgraph_postgres_starter.agents.incident_triage.nodes.analyze import validate_analysis
from langgraph_postgres_starter.agents.incident_triage.prompts import PROMPT_VERSION
from langgraph_postgres_starter.agents.incident_triage.schemas import (
    SERDE_TYPES,
    EvidenceCitation,
    IncidentMessage,
    Target,
    TriageAnalysis,
    Usage,
)
from langgraph_postgres_starter.agents.incident_triage.spec import spec
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.agents.incident_triage.tests.pipeline import (
    make_runtime,
    state_before_analyze,
)
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.persistence.serde import make_serde
from langgraph_postgres_starter.policies import default_retry_policy, step_timeout
from langgraph_postgres_starter.settings import Settings, load_settings
from langgraph_postgres_starter.worker.registry import builtin_agents, load_agents

REPO_ROOT = Path(__file__).parents[5]
AGENT_DIR = Path(__file__).parents[1]
FIXTURES = list_fixtures()
INFRA_FIXTURES = [fid for fid in FIXTURES if load_fixture(fid).expected["code"] is None]

# The nodes and their timeouts, in seconds (the plan's Task 33 table).
TIMEOUTS = {
    "resolve_target": 30,
    "collect_k8s": 60,
    "collect_datadog": 60,
    "assess": 10,
    "collect_code": 180,
    "analyze": 330,
    "publish": 30,
}
DATADOG_HANDLER = "__error_handler__collect_datadog"
CODE_HANDLER = "__error_handler__collect_code"

LANGGRAPH_JSON = {
    "python_version": "3.14",
    "dependencies": ["."],
    "graphs": {
        "incident_triage": "./src/langgraph_postgres_starter/agents/incident_triage/graph.py:graph"
    },
    "env": ".env",
}

# Fake secrets: none of them may reach a trace or a checkpoint.
SECRETS = {
    "dd_api_key": "SECRET-DD-API",
    "dd_app_key": "SECRET-DD-APP",
    "git_token": "SECRET-GIT",
    "database_url": "postgresql://lps:SECRET-DSN@db:5432/lps",
}
# The context's types: their reprs must not reach a trace or a checkpoint either.
CONTEXT_REPRS = ("TriageContext", "Settings", "SecretStr", "Adapters", "RunInfo")
# Nothing listens on port 1.
NO_NETWORK = "http://127.0.0.1:1"


@dataclass
class Run:
    """One run's graph input, config and context, as the worker builds them."""

    input: dict[str, Any]
    config: RunnableConfig
    context: TriageContext


def _run(fid: str, *, settings: Settings | None = None, **adapters: Any) -> Run:
    """A new run of the fixture incident ``fid``; ``adapters`` replace the context's adapters."""
    message = IncidentMessage.model_validate(load_fixture(fid).incident)
    info = RunInfo(run_id=new_id(), dedup_key=spec.dedup_key(message, datetime.now(UTC)), attempt=1)
    context: TriageContext = spec.make_context(info, message, settings or load_settings())
    if adapters:
        context = dataclasses.replace(
            context, adapters=dataclasses.replace(context.adapters, **adapters)
        )
    return Run(
        input=spec.initial_state(message),
        config={"configurable": {"thread_id": str(info.run_id)}},
        context=context,
    )


def _compile(saver: InMemorySaver | None = None) -> CompiledStateGraph[Any, Any, Any, Any]:
    return build_graph().compile(
        checkpointer=saver or InMemorySaver(serde=make_serde(spec.serde_types))
    )


async def _steps(graph: CompiledStateGraph[Any, Any, Any, Any], run: Run) -> list[str]:
    """The names of the nodes that ran, in order (``stream_mode="updates"``)."""
    return [
        node
        async for chunk in graph.astream(
            run.input, run.config, context=run.context, stream_mode="updates", durability="sync"
        )
        for node in chunk
    ]


def _note(state: dict[str, Any], note_id: str) -> Any:
    [note] = [item for item in state["notes"] if item.id == note_id]
    return note


@dataclass
class FailingDatadog:
    """Raises ``error(message)`` on every search, and counts the searches."""

    error: type[Exception]
    message: str
    calls: int = 0

    async def search_events(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> list[DdEvent]:
        self.calls += 1
        raise self.error(self.message)


@dataclass
class FailingLocator:
    """Raises ``error(message)`` on every lookup, and counts the lookups."""

    error: type[Exception]
    message: str
    calls: int = 0

    async def locate(self, target: Target, annotations: Any) -> SourceRef | None:
        self.calls += 1
        raise self.error(self.message)


@dataclass
class NoteLlm:
    """Answers every request with an analysis that cites the note ``note_id`` only."""

    note_id: str
    requests: list[AnalysisRequest] = field(default_factory=list)

    async def analyze(self, req: AnalysisRequest) -> LlmResponse:
        self.requests.append(req)
        analysis = TriageAnalysis(
            category="unknown",
            summary="Part of the evidence is missing.",
            root_cause="Not known.",
            confidence="low",
            evidence=[EvidenceCitation(id=self.note_id, why="A step failed.")],
            suspected_code=[],
            recommended_actions=[],
            needs_human=True,
        )
        return LlmResponse(
            text=analysis.model_dump_json(),
            stop_reason="end_turn",
            model_id="test-model",
            usage=Usage(input_tokens=1, output_tokens=1),
        )


class NoNetworkSession(requests.Session):
    """A requests session that fails every request."""

    def request(self, *args, **kwargs):
        raise AssertionError("a unit test tried to reach the network")


# --- full runs ------------------------------------------------------------------------


@pytest.mark.parametrize("fid", FIXTURES)
async def test_full_run_on_fixture(fid):
    run = _run(fid)

    result = cast(
        TriageState,
        await _compile().ainvoke(run.input, run.config, context=run.context, durability="sync"),
    )

    analysis = result["analysis"]
    assert analysis is not None
    assert result["refusal"] is False
    assert validate_analysis(analysis.model_dump_json(), result) == analysis
    assert analysis.category == load_fixture(fid).expected["category"]
    assert result["published"] == ["log"]


@pytest.mark.parametrize("fid", INFRA_FIXTURES)
async def test_infra_fixture_skips_code(fid):
    steps = await _steps(_compile(), _run(fid))

    assert "collect_code" not in steps
    assert steps == [
        "resolve_target",
        "collect_k8s",
        "collect_datadog",
        "assess",
        "analyze",
        "publish",
    ]


async def test_application_fixture_collects_code():
    steps = await _steps(_compile(), _run("python-keyerror"))

    assert steps == [*TIMEOUTS]


@pytest.mark.parametrize("fid", FIXTURES)
async def test_recorded_reply_validates(fid):  # ruling R25
    state = await state_before_analyze(fid, make_runtime(fid))
    reply = json.loads(
        (FIXTURES_DIR / "recorded" / fid / PROMPT_VERSION / "attempt-0.json").read_text()
    )
    expected = load_fixture(fid).expected

    analysis = validate_analysis(reply["text"], state)

    assert reply["stop_reason"] == "end_turn"
    assert analysis.category == expected["category"]
    code = expected["code"]
    if code is None:
        assert analysis.suspected_code == []
    else:
        assert any(
            cited.path == code["path"]
            and cited.start_line <= code["end_line"]
            and code["start_line"] <= cited.end_line
            for cited in analysis.suspected_code
        )
    text = f"{analysis.summary}\n{analysis.root_cause}".lower()
    assert [fact for fact in expected["facts"] if fact.lower() not in text] == []


# --- policies and error handlers (P12) --------------------------------------------------


def test_node_policies():
    graph = build_graph().compile()

    for name, seconds in TIMEOUTS.items():
        node = graph.nodes[name]
        assert node.timeout == step_timeout(seconds), name
        assert node.retry_policy == (default_retry_policy(),), name
    assert graph.nodes["collect_datadog"].error_handler_node == DATADOG_HANDLER
    assert graph.nodes["collect_code"].error_handler_node == CODE_HANDLER
    others = set(TIMEOUTS) - {"collect_datadog", "collect_code"}
    assert all(graph.nodes[name].error_handler_node is None for name in others)
    # The handlers get the graph's defaults.
    for handler in (DATADOG_HANDLER, CODE_HANDLER):
        assert graph.nodes[handler].timeout == step_timeout(120)
        assert graph.nodes[handler].retry_policy == (default_retry_policy(),)


async def test_datadog_failure_continues_with_note():
    datadog = FailingDatadog(RuntimeError, "403 Forbidden: api_key=SECRET-DD-API")
    llm = NoteLlm("note:datadog:unavailable")
    run = _run("python-keyerror", datadog=datadog, llm=llm)
    graph = _compile()

    steps = await _steps(graph, run)

    assert steps == [
        "resolve_target",
        "collect_k8s",
        DATADOG_HANDLER,
        "assess",
        "collect_code",
        "analyze",
        "publish",
    ]
    # RuntimeError is not transient: the retry policy does not run the step again.
    assert datadog.calls == 1
    state = (await graph.aget_state(run.config)).values
    note = _note(state, "note:datadog:unavailable")
    assert note.kind == "note"
    assert "RuntimeError" in note.text
    assert "403" not in note.text and "SECRET" not in note.text
    assert "datadog_evidence" not in state
    [request] = llm.requests
    assert "[note:datadog:unavailable]" in request.user
    assert state["analysis"].evidence[0].id == "note:datadog:unavailable"
    assert state["published"] == ["log"]


async def test_transient_datadog_error_is_retried_before_the_handler():
    # The retry policy waits about 0.5 s and 1 s (plus jitter) between the attempts.
    datadog = FailingDatadog(ConnectionError, "connection reset by peer")
    run = _run("python-keyerror", datadog=datadog, llm=NoteLlm("note:datadog:unavailable"))
    graph = _compile()

    steps = await _steps(graph, run)

    assert datadog.calls == default_retry_policy().max_attempts == 3
    assert steps.count(DATADOG_HANDLER) == 1
    state = (await graph.aget_state(run.config)).values
    assert "ConnectionError" in _note(state, "note:datadog:unavailable").text


async def test_code_failure_continues_with_note():
    locator = FailingLocator(RuntimeError, "git token SECRET-GIT was refused")
    llm = NoteLlm("note:code:unavailable")
    run = _run("python-keyerror", locator=locator, llm=llm)
    graph = _compile()

    steps = await _steps(graph, run)

    assert steps == [
        "resolve_target",
        "collect_k8s",
        "collect_datadog",
        "assess",
        CODE_HANDLER,
        "analyze",
        "publish",
    ]
    assert locator.calls == 1
    state = (await graph.aget_state(run.config)).values
    note = _note(state, "note:code:unavailable")
    assert "RuntimeError" in note.text
    assert "SECRET" not in note.text and "refused" not in note.text
    assert "code_evidence" not in state
    [request] = llm.requests
    assert "[note:code:unavailable]" in request.user


async def test_cancelled_step_runs_no_handler():
    # A run that is cancelled (as by the worker's run deadline) stops in its step; the
    # error handler does not turn the cancellation into a note.
    run = _run("python-keyerror", datadog=FakeDatadog(delay_s=60))
    graph = _compile()

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.5):
            await graph.ainvoke(run.input, run.config, context=run.context, durability="sync")

    snapshot = await graph.aget_state(run.config)
    assert snapshot.next == ("collect_datadog",)
    assert all(note.id != "note:datadog:unavailable" for note in snapshot.values["notes"])


# --- context, traces and checkpoints (P14, N43) ------------------------------------------


class RecordingClient(langsmith.Client):
    """A langsmith client that keeps each run it would create or update, and sends nothing."""

    def __init__(self) -> None:
        super().__init__(
            api_url=NO_NETWORK,
            api_key="test-key",
            session=NoNetworkSession(),
            auto_batch_tracing=False,
        )
        self.created: list[dict[str, Any]] = []
        self.updated: list[dict[str, Any]] = []

    def create_run(self, *args, **kwargs):
        self.created.append(kwargs)

    def update_run(self, *args, **kwargs):
        self.updated.append(kwargs)


async def test_context_stays_out_of_traces_and_checkpoints():
    run = _run("python-keyerror", settings=load_settings(**SECRETS))
    thread_id = run.config["configurable"]["thread_id"]
    client = RecordingClient()
    # The tracer that LangSmith tracing adds (LANGSMITH_TRACING), with the client above.
    tracer = LangChainTracer(client=client, project_name="test")
    saver = InMemorySaver(serde=make_serde(spec.serde_types))
    graph = _compile(saver)

    try:
        await graph.ainvoke(
            run.input,
            {**run.config, "callbacks": [tracer]},
            context=run.context,
            durability="sync",
        )
    finally:
        client.close(timeout=0)

    # Every node was traced. A run's metadata holds what langchain-core copies from
    # configurable, which is the thread ID only, and LangGraph's own keys.
    assert set(TIMEOUTS) <= {traced["name"] for traced in client.created}
    for traced in client.created:
        metadata = traced["extra"]["metadata"]
        assert metadata["thread_id"] == thread_id
        assert {
            key for key in metadata if not key.startswith(("langgraph_", "ls_", "checkpoint_"))
        } <= {"thread_id"}
    traces = repr([client.created, client.updated])
    checkpoints = repr(
        [
            (tup.config, tup.checkpoint, tup.metadata, tup.pending_writes)
            async for tup in saver.alist(run.config)
        ]
    )
    for stored in (traces, checkpoints):
        assert [secret for secret in SECRETS.values() if secret in stored] == []
        assert [name for name in CONTEXT_REPRS if name in stored] == []
    snapshot = await graph.aget_state(run.config)
    assert set(snapshot.config["configurable"]) == {"thread_id", "checkpoint_ns", "checkpoint_id"}


# --- registration, the agent spec and Studio (P22) ---------------------------------------


def test_registry_includes_incident_triage():
    assert spec in builtin_agents()
    assert load_agents(load_settings())["incident_triage"] is spec
    assert spec.name == "incident_triage"
    assert spec.input_model is IncidentMessage
    assert tuple(spec.serde_types) == SERDE_TYPES
    assert spec.migrations_dir == AGENT_DIR / "migrations"
    assert (spec.migrations_dir / "0001_triage_results.sql").is_file()
    assert spec.retention_sql == "delete from triage_results where created_at < %(cutoff)s"
    assert isinstance(spec.build_graph().compile(), CompiledStateGraph)


class OtherInput(BaseModel):
    key: str


def test_spec_functions_take_an_incident_message():  # N3
    message = IncidentMessage.model_validate(load_fixture("python-keyerror").incident)
    now = datetime.now(UTC)
    info = RunInfo(run_id=new_id(), dedup_key="d", attempt=1)

    assert spec.dedup_key(message, now) == dedup_key(message, now)
    assert spec.initial_state(message) == {"message": message}
    context = spec.make_context(info, message, load_settings())
    assert isinstance(context, TriageContext)
    assert (context.run, context.fixture_id) == (info, "python-keyerror")

    other = OtherInput(key="a")
    with pytest.raises(TypeError, match="IncidentMessage"):
        spec.dedup_key(other, now)
    with pytest.raises(TypeError, match="IncidentMessage"):
        spec.initial_state(other)
    with pytest.raises(TypeError, match="IncidentMessage"):
        spec.make_context(info, other, load_settings())


def test_langgraph_json_graph_imports():
    config = json.loads((REPO_ROOT / "langgraph.json").read_text())
    assert config == LANGGRAPH_JSON

    # Load the graph's file as `langgraph dev` does: by its path.
    path, _, attr = config["graphs"]["incident_triage"].partition(":")
    module_spec = importlib.util.spec_from_file_location("studio_incident_triage", REPO_ROOT / path)
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    studio_graph = getattr(module, attr)

    assert isinstance(studio_graph, CompiledStateGraph)
    # Compiled without a checkpointer: Studio supplies its own (spec §10.3).
    assert studio_graph.checkpointer is None
    assert set(TIMEOUTS) <= set(studio_graph.nodes)


async def test_module_graph_runs_without_a_context():
    # As in Studio: no context, so each step uses default_context(), which serves the
    # fixture APP_STUDIO_FIXTURE (python-keyerror) with a new run ID per call (N44).
    message = IncidentMessage.model_validate(load_fixture("python-keyerror").incident)

    result = await graph_module.graph.ainvoke({"message": message})

    assert result["analysis"].category == "application"
    assert result["published"] == ["log"]
