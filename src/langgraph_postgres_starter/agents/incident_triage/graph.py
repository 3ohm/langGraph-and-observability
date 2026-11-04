"""The Incident Triage graph (spec §8.5, §9.2, §10.3).

``resolve_target`` → ``collect_k8s`` → ``collect_datadog`` → ``assess`` →
(``collect_code`` if the assessment says so) → ``analyze`` → ``publish`` → END.

Policies (P12):

- Every node retries what ``policies.is_transient`` accepts (``default_retry_policy``)
  and caps each attempt with its own timeout (``TIMEOUTS``); the graph-wide default,
  120 s, applies to the error handlers.
- ``collect_datadog`` and ``collect_code`` have error handlers. When the step fails for
  good (an error that is not transient, or the last attempt), its handler adds the
  note ``note:datadog:unavailable`` or ``note:code:unavailable`` and the run goes on:
  to ``assess`` after Datadog, to ``analyze`` after the code. The note names the
  error's type only, never its message, which may hold what the analysis must not
  see. LangGraph calls no handler for an interrupt or a drain (``GraphBubbleUp``), nor
  when the run itself is cancelled, as by the worker's run deadline.

``graph`` is the graph compiled without a checkpointer, for Studio (``langgraph.json``),
which supplies its own. Invoked without a context, each step uses ``default_context()``
(spec §10.3).
"""

from typing import Literal

from langgraph.errors import NodeError
from langgraph.graph import START, StateGraph
from langgraph.types import Command

from langgraph_postgres_starter.agents.incident_triage.assess import (
    assess_node,
    route_after_assess,
)
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext
from langgraph_postgres_starter.agents.incident_triage.evidence import make_id, make_item
from langgraph_postgres_starter.agents.incident_triage.nodes.analyze import analyze
from langgraph_postgres_starter.agents.incident_triage.nodes.code import collect_code
from langgraph_postgres_starter.agents.incident_triage.nodes.datadog import collect_datadog
from langgraph_postgres_starter.agents.incident_triage.nodes.k8s import collect_k8s
from langgraph_postgres_starter.agents.incident_triage.nodes.publish import publish
from langgraph_postgres_starter.agents.incident_triage.nodes.resolve import resolve_target
from langgraph_postgres_starter.agents.incident_triage.schemas import EvidenceItem
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.logging import get_logger
from langgraph_postgres_starter.policies import default_retry_policy, step_timeout

_log = get_logger(__name__)

# The longest one attempt of each node may take, in seconds.
TIMEOUTS: dict[str, float] = {
    "resolve_target": 30,
    "collect_k8s": 60,
    "collect_datadog": 60,
    "assess": 10,
    "collect_code": 180,
    "analyze": 330,
    "publish": 30,
}
# The default timeout of a node without its own: the error handlers.
DEFAULT_TIMEOUT_S = 120


async def datadog_unavailable(state: TriageState, error: NodeError) -> Command[Literal["assess"]]:
    """``collect_datadog``'s error handler: note that Datadog failed, then go to ``assess``."""
    note = _unavailable(error, "datadog", "Datadog events unavailable", "Datadog events")
    return Command(update={"notes": [note]}, goto="assess")


async def code_unavailable(state: TriageState, error: NodeError) -> Command[Literal["analyze"]]:
    """``collect_code``'s error handler: note that the code step failed, then go to ``analyze``."""
    note = _unavailable(error, "code", "Code evidence unavailable", "code evidence")
    return Command(update={"notes": [note]}, goto="analyze")


def build_graph() -> StateGraph[TriageState, TriageContext, TriageState, TriageState]:
    """The graph, not compiled. The worker compiles it with its checkpointer."""
    builder = StateGraph(TriageState, context_schema=TriageContext)
    builder.set_node_defaults(
        retry_policy=default_retry_policy(), timeout=step_timeout(DEFAULT_TIMEOUT_S)
    )
    builder.add_node(resolve_target, timeout=step_timeout(TIMEOUTS["resolve_target"]))
    builder.add_node(collect_k8s, timeout=step_timeout(TIMEOUTS["collect_k8s"]))
    builder.add_node(
        collect_datadog,
        timeout=step_timeout(TIMEOUTS["collect_datadog"]),
        error_handler=datadog_unavailable,
    )
    builder.add_node("assess", assess_node, timeout=step_timeout(TIMEOUTS["assess"]))
    builder.add_node(
        collect_code,
        timeout=step_timeout(TIMEOUTS["collect_code"]),
        error_handler=code_unavailable,
    )
    builder.add_node(analyze, timeout=step_timeout(TIMEOUTS["analyze"]))
    builder.add_node(publish, timeout=step_timeout(TIMEOUTS["publish"]))
    builder.add_edge(START, "resolve_target")
    builder.add_edge("resolve_target", "collect_k8s")
    builder.add_edge("collect_k8s", "collect_datadog")
    builder.add_edge("collect_datadog", "assess")
    builder.add_conditional_edges("assess", route_after_assess)
    builder.add_edge("collect_code", "analyze")
    builder.add_edge("analyze", "publish")
    return builder


def _unavailable(error: NodeError, step: str, title: str, what: str) -> EvidenceItem:
    """The note of a step that failed for good. It names the error's type only."""
    kind = type(error.error).__name__
    _log.warning("step.unavailable", node=error.node, error_type=kind)
    return make_item(
        make_id("note", step, "unavailable"),
        "note",
        title,
        f"The step {error.node} failed with {kind}, so the analysis has no {what}. "
        "The run went on without them.",
    )


graph = build_graph().compile()
