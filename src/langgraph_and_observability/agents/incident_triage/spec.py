"""The Incident Triage agent as the worker sees it: its ``AgentSpec`` (spec §8.5, §9, §13.9).

``AgentSpec`` types the queue message as any pydantic model. The functions here take
that model, check that it is an ``IncidentMessage`` (TypeError otherwise), and pass it
on to the agent's own typed functions.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from langgraph_and_observability.agent_spec import AgentSpec, RunInfo
from langgraph_and_observability.agents.incident_triage import context, dedup
from langgraph_and_observability.agents.incident_triage.graph import build_graph
from langgraph_and_observability.agents.incident_triage.schemas import SERDE_TYPES, IncidentMessage
from langgraph_and_observability.agents.incident_triage.tracing import trace_metadata
from langgraph_and_observability.settings import Settings

# The agent's SQL migrations: its triage_results table.
MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _incident(model: BaseModel) -> IncidentMessage:
    if not isinstance(model, IncidentMessage):
        raise TypeError(f"incident_triage takes an IncidentMessage, not a {type(model).__name__}")
    return model


def dedup_key(model: BaseModel, received_at: datetime) -> str:
    """The run's dedup key (``dedup.dedup_key``)."""
    return dedup.dedup_key(_incident(model), received_at)


def make_context(info: RunInfo, model: BaseModel, settings: Settings) -> context.TriageContext:
    """The context of one invocation (``context.make_context``)."""
    return context.make_context(info, _incident(model), settings)


def initial_state(model: BaseModel) -> dict[str, Any]:
    """The graph's input for a new run: the message."""
    return {"message": _incident(model)}


def required_settings(settings: Settings) -> list[str]:
    """The settings that a ``prod`` worker needs for this agent: the Datadog keys in real mode."""
    return ["dd_api_key", "dd_app_key"] if settings.adapters == "real" else []


spec = AgentSpec(
    name="incident_triage",
    build_graph=build_graph,
    input_model=IncidentMessage,
    dedup_key=dedup_key,
    make_context=make_context,
    initial_state=initial_state,
    serde_types=SERDE_TYPES,
    migrations_dir=MIGRATIONS_DIR,
    retention_sql="delete from triage_results where created_at < %(cutoff)s",
    required_settings=required_settings,
    trace_metadata=trace_metadata,
)
