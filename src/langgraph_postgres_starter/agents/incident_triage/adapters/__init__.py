"""The agent's adapters: one interface per external system, each with a fake (spec §9, §10.2).

Each subpackage holds one interface (``interface.py``) and the adapters that implement
it. ``build_adapters`` picks them from the settings:

- ``APP_ADAPTERS`` picks Kubernetes, Datadog and the source adapters. ``simulated``
  serves an incident fixture (``fixtures/FORMAT.md``).
- ``APP_LLM_MODE`` picks the model. ``recorded`` replies with recorded responses.
- ``APP_RESULT_SINKS`` lists the sinks. ``log`` is the same in every mode.
"""

from dataclasses import dataclass
from uuid import UUID

from langgraph_postgres_starter.agents.incident_triage.adapters.call_log import CallLog
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.fake import FakeDatadog
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.interface import (
    DatadogAdapter,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.fake import FakeK8s
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.interface import K8sAdapter
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import LlmAdapter
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.recorded import RecordedLlm
from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.interface import ResultSink
from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.log import LogSink
from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import (
    SourceLocator,
    SourceProvider,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.local import (
    FixtureLocator,
    LocalSourceProvider,
)
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_postgres_starter.settings import Settings


@dataclass(frozen=True)
class Adapters:
    """The adapters of one run. ``sinks`` follow the order of ``APP_RESULT_SINKS``."""

    k8s: K8sAdapter
    datadog: DatadogAdapter
    locator: SourceLocator
    source: SourceProvider
    llm: LlmAdapter
    sinks: tuple[ResultSink, ...]


# The real adapters that are not built yet, each with the task of the implementation
# plan (docs/plans/2026-11-02-incident-triage-v1.md) that adds it. The task that adds
# an adapter removes its entry.
_NOT_BUILT_YET: dict[str, int] = {
    "k8s": 23,
    "datadog": 24,
    "locator": 28,
    "source": 28,
    "llm": 31,
    "sink.sqs": 32,
}


def build_adapters(
    settings: Settings, *, fixture_id: str | None, run_id: UUID | None = None
) -> Adapters:
    """Build a run's adapters from ``settings``.

    In simulated mode the fakes serve the incident fixture ``fixture_id`` from
    ``settings.fixtures_dir``. With ``settings.fake_call_log_dir`` and ``run_id`` set,
    every adapter appends its calls to the run's call log (``call_log.py``).
    Constructing an adapter touches no network.

    Raises NotImplementedError when the settings select a real adapter that is not
    built yet; the message names each such adapter and its task, as in ``k8s (Task
    23)``. In simulated mode, raises ValueError without a ``fixture_id`` and
    FileNotFoundError when the fixture is missing.
    """
    not_built = [name for name in _real_adapters(settings) if name in _NOT_BUILT_YET]
    if not_built:
        tasks = ", ".join(f"{name} (Task {_NOT_BUILT_YET[name]})" for name in not_built)
        raise NotImplementedError(
            f"the settings select real adapters that are not built yet: {tasks}. The task "
            "numbers are those of docs/plans/2026-11-02-incident-triage-v1.md. Use "
            'APP_ADAPTERS=simulated, APP_LLM_MODE=recorded and APP_RESULT_SINKS=["log"].'
        )
    # Every real adapter is unbuilt, so from here the mode is simulated, the model is
    # recorded and every sink is log.
    if fixture_id is None:
        raise ValueError(
            "simulated adapters (APP_ADAPTERS=simulated) serve an incident fixture, but "
            "there is no fixture ID: set raw.fixture_id in the message to the ID of a "
            f"fixture in {settings.fixtures_dir / 'incidents'}"
        )
    fixture = load_fixture(fixture_id, root=settings.fixtures_dir)
    calls = CallLog.for_run(settings, run_id)
    return Adapters(
        k8s=FakeK8s.from_fixture(fixture.k8s, calls),
        datadog=FakeDatadog.from_fixture(
            fixture.datadog, delay_s=settings.fake_datadog_delay_s, calls=calls
        ),
        locator=FixtureLocator.from_fixture(fixture.source, calls),
        source=LocalSourceProvider(settings.fixtures_dir / "apps", calls),
        llm=RecordedLlm(settings.fixtures_dir / "recorded", calls),
        sinks=tuple(LogSink(calls) for _ in settings.result_sinks),
    )


def _real_adapters(settings: Settings) -> list[str]:
    """The adapters that the settings select in their real form, by name."""
    names = ["k8s", "datadog", "locator", "source"] if settings.adapters == "real" else []
    if settings.llm_mode == "bedrock":
        names.append("llm")
    names += [f"sink.{sink}" for sink in settings.result_sinks if sink != "log"]
    return names
