"""The run context of the Incident Triage graph: its ``context_schema`` (spec §8.5, §10.3).

The context holds one invocation's fixed inputs: the run, the settings and the
adapters. It is built again for every invocation and is never checkpointed. Nodes read
it with ``ctx(runtime, message)``.

The worker builds it with ``make_context``, which gives every field. Studio (``langgraph
dev``) sends the run's context as a dict, such as ``{}`` or ``{"fixture_id": "<id>"}``,
and LangGraph builds ``TriageContext(**that_dict)``: each field it leaves out gets its
Studio default (``TriageContext``). The simulated adapters then serve the fixture that
the context names, else the one that the message's ``raw.fixture_id`` names (``ctx``),
else ``APP_STUDIO_FIXTURE``.
"""

from dataclasses import dataclass, field

from langgraph.runtime import Runtime

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.agents.incident_triage.adapters import Adapters, build_adapters
from langgraph_postgres_starter.agents.incident_triage.schemas import IncidentMessage
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.settings import Settings, get_settings

# The dedup key of a run that the worker did not start, as in Studio.
STUDIO_DEDUP_KEY = "studio"


@dataclass(frozen=True, init=False)
class TriageContext:
    """The graph's context. ``fixture_id`` names the incident fixture of a simulated run.

    Every argument may be left out, as in Studio. A missing one is built here, once:

    - ``settings``: ``get_settings()``;
    - ``run``: a new run, with the dedup key ``studio`` and attempt 1;
    - ``adapters``: ``build_adapters`` for ``fixture_id``, else ``APP_STUDIO_FIXTURE``,
      under the run's ID. ``fixture_id`` is then that fixture.

    A context given neither ``adapters`` nor ``fixture_id`` is open: the message may
    pick its fixture (``for_message``). The worker's ``make_context`` gives every
    argument, so nothing is built and the message picks nothing.
    """

    run: RunInfo
    settings: Settings
    adapters: Adapters
    fixture_id: str | None
    # Whether the message's raw.fixture_id may pick the fixture.
    _open: bool = field(init=False, repr=False, compare=False)
    # The contexts that messages picked, by fixture ID (for_message).
    _picked: dict[str, TriageContext] = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        run: RunInfo | None = None,
        settings: Settings | None = None,
        adapters: Adapters | None = None,
        fixture_id: str | None = None,
    ) -> None:
        settings = get_settings() if settings is None else settings
        if run is None:
            run = RunInfo(run_id=new_id(), dedup_key=STUDIO_DEDUP_KEY, attempt=1)
        is_open = adapters is None and fixture_id is None
        if adapters is None:
            fixture_id = fixture_id or settings.studio_fixture
            adapters = build_adapters(settings, fixture_id=fixture_id, run_id=run.run_id)
        # A frozen dataclass sets its fields with object.__setattr__.
        object.__setattr__(self, "run", run)
        object.__setattr__(self, "settings", settings)
        object.__setattr__(self, "adapters", adapters)
        object.__setattr__(self, "fixture_id", fixture_id)
        object.__setattr__(self, "_open", is_open)
        object.__setattr__(self, "_picked", {})

    def for_message(self, message: IncidentMessage) -> TriageContext:
        """This context, or, when it is open, the one for the message's fixture.

        The context for a fixture has this context's run and settings. It is built on
        the first call and kept, so every step of a run gets the same adapters.
        """
        wanted = _fixture_id(message)
        if not self._open or wanted is None or wanted == self.fixture_id:
            return self
        picked = self._picked.get(wanted)
        if picked is None:
            picked = TriageContext(run=self.run, settings=self.settings, fixture_id=wanted)
            self._picked[wanted] = picked
        return picked


def make_context(info: RunInfo, msg: IncidentMessage, settings: Settings) -> TriageContext:
    """Build the context of one invocation of the run ``info`` (the agent's ``make_context``).

    The fixture ID is the message's ``raw["fixture_id"]``, if it is a string. The
    adapters write their call log under the run's ID (``build_adapters``).
    """
    fixture_id = _fixture_id(msg)
    adapters = build_adapters(settings, fixture_id=fixture_id, run_id=info.run_id)
    return TriageContext(run=info, settings=settings, adapters=adapters, fixture_id=fixture_id)


def default_context() -> TriageContext:
    """The context of a run that has none (spec §10.3): ``TriageContext()``.

    It serves the fixture ``APP_STUDIO_FIXTURE`` (``studio_fixture``), with a new run
    ID on each call, the dedup key ``studio`` and attempt 1.
    """
    return TriageContext()


def ctx(runtime: Runtime[TriageContext], message: IncidentMessage | None) -> TriageContext:
    """The node's context for the run's ``message``.

    It is ``runtime.context``, or ``default_context()`` when the run has none, and then
    ``for_message(message)``: an open context, as Studio's, serves the fixture that the
    message's ``raw.fixture_id`` names. Without a message (a step called on a partial
    state, as in a test) it is the context itself.
    """
    context = default_context() if runtime.context is None else runtime.context
    return context if message is None else context.for_message(message)


def _fixture_id(msg: IncidentMessage) -> str | None:
    if msg.raw is None:
        return None
    fixture_id = msg.raw.get("fixture_id")
    return fixture_id if isinstance(fixture_id, str) else None
