"""The run context of the Incident Triage graph: its ``context_schema`` (spec §8.5, §10.3).

The context holds one invocation's fixed inputs: the run, the settings and the
adapters. It is built again for every invocation and is never checkpointed. Nodes read
it with ``ctx(runtime)``.
"""

from dataclasses import dataclass

from langgraph.runtime import Runtime

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.agents.incident_triage.adapters import Adapters, build_adapters
from langgraph_postgres_starter.agents.incident_triage.schemas import IncidentMessage
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.settings import Settings, get_settings


@dataclass(frozen=True)
class TriageContext:
    """The graph's context. ``fixture_id`` names the incident fixture of a simulated run."""

    run: RunInfo
    settings: Settings
    adapters: Adapters
    fixture_id: str | None


def make_context(info: RunInfo, msg: IncidentMessage, settings: Settings) -> TriageContext:
    """Build the context of one invocation of the run ``info`` (the agent's ``make_context``).

    The fixture ID is the message's ``raw["fixture_id"]``, if it is a string. The
    adapters write their call log under the run's ID (``build_adapters``).
    """
    fixture_id = _fixture_id(msg)
    adapters = build_adapters(settings, fixture_id=fixture_id, run_id=info.run_id)
    return TriageContext(run=info, settings=settings, adapters=adapters, fixture_id=fixture_id)


def default_context() -> TriageContext:
    """The context of a run that has none, as in Studio (spec §10.3).

    It serves the fixture ``APP_STUDIO_FIXTURE`` (``studio_fixture``), with a new run
    ID on each call, the dedup key ``studio`` and attempt 1.
    """
    settings = get_settings()
    run = RunInfo(run_id=new_id(), dedup_key="studio", attempt=1)
    fixture_id = settings.studio_fixture
    adapters = build_adapters(settings, fixture_id=fixture_id, run_id=run.run_id)
    return TriageContext(run=run, settings=settings, adapters=adapters, fixture_id=fixture_id)


def ctx(runtime: Runtime[TriageContext]) -> TriageContext:
    """The node's context: ``runtime.context``, or ``default_context()`` if it is not set."""
    context = runtime.context
    return default_context() if context is None else context


def _fixture_id(msg: IncidentMessage) -> str | None:
    if msg.raw is None:
        return None
    fixture_id = msg.raw.get("fixture_id")
    return fixture_id if isinstance(fixture_id, str) else None
