"""The steps before ``analyze`` on an incident fixture, as a simulated run does them (test support).

``state_before_analyze`` runs ``resolve_target``, ``collect_k8s``, ``collect_datadog``,
``assess`` and, when the assessment says so, ``collect_code``. It appends each step's
notes, as the graph's ``operator.add`` reducer does. ``RecordingLlm`` keeps the requests
that a model adapter gets.
"""

import dataclasses
from dataclasses import dataclass, field

from langgraph.runtime import Runtime

from langgraph_and_observability.agent_spec import RunInfo
from langgraph_and_observability.agents.incident_triage.adapters import build_adapters
from langgraph_and_observability.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmAdapter,
    LlmResponse,
)
from langgraph_and_observability.agents.incident_triage.assess import assess_node
from langgraph_and_observability.agents.incident_triage.context import TriageContext
from langgraph_and_observability.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_and_observability.agents.incident_triage.nodes.code import collect_code
from langgraph_and_observability.agents.incident_triage.nodes.datadog import collect_datadog
from langgraph_and_observability.agents.incident_triage.nodes.k8s import collect_k8s
from langgraph_and_observability.agents.incident_triage.nodes.resolve import resolve_target
from langgraph_and_observability.agents.incident_triage.schemas import IncidentMessage
from langgraph_and_observability.agents.incident_triage.state import TriageState
from langgraph_and_observability.ids import new_id
from langgraph_and_observability.settings import load_settings

# The incident that the other adapters serve for a fixture ID below _test/, which names
# recorded replies only.
_TEST_INCIDENT = "python-keyerror"


def make_runtime(fixture_id: str, *, llm: LlmAdapter | None = None) -> Runtime[TriageContext]:
    """The runtime of a simulated run of ``fixture_id``, with ``llm`` as its model if given.

    A fixture ID below ``_test/`` has recorded replies but no incident. Its runtime's
    other adapters serve ``python-keyerror``; the tests that use it call only the model.
    """
    settings = load_settings()
    incident = _TEST_INCIDENT if fixture_id.startswith("_test/") else fixture_id
    adapters = build_adapters(settings, fixture_id=incident)
    if llm is not None:
        adapters = dataclasses.replace(adapters, llm=llm)
    run = RunInfo(run_id=new_id(), dedup_key="test", attempt=1)
    return Runtime(
        context=TriageContext(run=run, settings=settings, adapters=adapters, fixture_id=fixture_id)
    )


async def state_before_analyze(fixture_id: str, runtime: Runtime[TriageContext]) -> TriageState:
    """The state that ``analyze`` gets in a simulated run of the incident ``fixture_id``."""
    message = IncidentMessage.model_validate(load_fixture(fixture_id).incident)
    state: TriageState = {"message": message, "notes": []}
    steps = [resolve_target, collect_k8s, collect_datadog, assess_node]
    for step in steps:
        _apply(state, await step(state, runtime))
    if state["assessment"].collect_code:
        _apply(state, await collect_code(state, runtime))
    return state


def _apply(state: TriageState, update: TriageState) -> None:
    notes = update.pop("notes", [])
    state.update(update)
    state["notes"] = state.get("notes", []) + notes


@dataclass
class RecordingLlm:
    """Passes each request to ``inner`` and keeps it in ``requests``."""

    inner: LlmAdapter
    requests: list[AnalysisRequest] = field(default_factory=list)

    async def analyze(self, req: AnalysisRequest) -> LlmResponse:
        self.requests.append(req)
        return await self.inner.analyze(req)
