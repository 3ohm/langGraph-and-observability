"""The agents that this worker can run (spec §13.9).

The registry holds the built-in agents, then the specs a caller passes in, then each
``"module:attr"`` entry of ``APP_EXTRA_AGENTS``. An entry names a module-level
``AgentSpec``, for example ``APP_EXTRA_AGENTS=["my_pkg.graph:SPEC"]``, so a fork can add
an agent without editing this file.
"""

import importlib
from collections.abc import Sequence

from langgraph_postgres_starter.agent_spec import AgentSpec
from langgraph_postgres_starter.agents.incident_triage.spec import spec as incident_triage_spec
from langgraph_postgres_starter.settings import Settings


def builtin_agents() -> list[AgentSpec]:
    """Return the agents that ship with this repository."""
    return [
        incident_triage_spec,
    ]


def load_agents(settings: Settings, *, include: Sequence[AgentSpec] = ()) -> dict[str, AgentSpec]:
    """Return every agent by name, in order: built-in, ``include``, ``settings.extra_agents``.

    Raises ValueError for an ``extra_agents`` entry that is not ``"module:attr"``, for an
    attribute that is missing or is not an ``AgentSpec``, and for two agents with one
    name. An entry whose module cannot be imported raises that import error.
    """
    specs = [*builtin_agents(), *include, *map(_import_spec, settings.extra_agents)]
    agents: dict[str, AgentSpec] = {}
    for spec in specs:
        if spec.name in agents:
            raise ValueError(f"duplicate agent name {spec.name!r}")
        agents[spec.name] = spec
    return agents


def _import_spec(entry: str) -> AgentSpec:
    """Import the ``AgentSpec`` that an ``APP_EXTRA_AGENTS`` entry names."""
    module_name, _, attr = entry.partition(":")
    if not attr.isidentifier() or not all(part.isidentifier() for part in module_name.split(".")):
        raise ValueError(f"APP_EXTRA_AGENTS entry {entry!r} is not 'module:attr'")
    module = importlib.import_module(module_name)
    try:
        spec = getattr(module, attr)
    except AttributeError:
        raise ValueError(
            f"APP_EXTRA_AGENTS entry {entry!r}: module {module_name!r} has no attribute {attr!r}"
        ) from None
    if not isinstance(spec, AgentSpec):
        raise ValueError(
            f"APP_EXTRA_AGENTS entry {entry!r}: {attr} is a {type(spec).__name__}, not an AgentSpec"
        )
    return spec
