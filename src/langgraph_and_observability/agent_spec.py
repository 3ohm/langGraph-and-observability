"""What the worker needs to know about an agent (spec §8.5, §13.9).

An agent is a plain LangGraph ``StateGraph`` plus the few functions that the worker
calls around it: to validate a queue message, to compute its dedup key, and to build
the run's context and first state. The worker reads agents from the registry
(``worker/registry.py``); this module imports nothing from the worker or the agents.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from langgraph.graph import StateGraph
from pydantic import BaseModel

from langgraph_and_observability.settings import Settings


@dataclass(frozen=True)
class RunInfo:
    """The run that one graph invocation belongs to.

    ``run_id`` is the run's UUIDv7 and the graph's ``thread_id``. ``attempt`` counts the
    run's invocations, from 1.
    """

    run_id: UUID
    dedup_key: str
    attempt: int


@dataclass(frozen=True)
class AgentSpec:  # pylint: disable=too-many-instance-attributes  # one per part of an agent
    """One agent: its graph and the functions the worker calls around it.

    - ``name``: the agent's name in the registry and in ``runs.agent``. It is unique.
    - ``build_graph``: returns the graph, not compiled. The worker compiles it with its
      own checkpointer.
    - ``input_model``: validates a queue message's body.
    - ``dedup_key``: maps the validated input and the time the message was sent to the
      run's dedup key. Messages with the same key map to one run.
    - ``make_context``: builds the graph's ``context_schema`` value for one invocation:
      the run's fixed inputs (spec §8.5). It never holds secrets.
    - ``initial_state``: the graph's input for a new run.
    - ``serde_types``: every type, other than the serializer's built-in safe types,
      that the graph stores in its state. The checkpoint serializer revives only these
      (``persistence.serde.make_serde``), so each must be defined at module level.
    - ``migrations_dir``: the agent's own SQL migrations, if it has tables.
    - ``retention_sql``: a statement that deletes the agent's old rows. It takes one
      parameter, ``%(cutoff)s``, the oldest time to keep.
    """

    name: str
    build_graph: Callable[[], StateGraph[Any, Any, Any, Any]]
    input_model: type[BaseModel]
    dedup_key: Callable[[BaseModel, datetime], str]
    make_context: Callable[[RunInfo, BaseModel, Settings], Any]
    initial_state: Callable[[BaseModel], dict[str, Any]]
    serde_types: Sequence[type]
    migrations_dir: Path | None = None
    retention_sql: str | None = None
