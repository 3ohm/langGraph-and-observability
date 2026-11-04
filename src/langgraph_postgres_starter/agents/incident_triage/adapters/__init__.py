"""The agent's adapters: one interface per external system, each with a fake (spec §9, §10.2).

Each subpackage holds one interface (``interface.py``) and the adapters that implement
it. ``build_adapters`` picks them from the settings:

- ``APP_ADAPTERS`` picks Kubernetes, Datadog and the source adapters. ``simulated``
  serves an incident fixture (``fixtures/FORMAT.md``).
- ``APP_LLM_MODE`` picks the model. ``recorded`` replies with recorded responses.
- ``APP_RESULT_SINKS`` lists the sinks. ``log`` is the same in every mode.
"""

import functools
from dataclasses import dataclass
from uuid import UUID

from langgraph_postgres_starter.agents.incident_triage.adapters.call_log import CallLog
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.fake import FakeDatadog
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.interface import (
    DatadogAdapter,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.real import RealDatadog
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.fake import FakeK8s
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.interface import K8sAdapter
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.real import RealK8s
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import LlmAdapter
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.recorded import RecordedLlm
from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.interface import ResultSink
from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.log import LogSink
from langgraph_postgres_starter.agents.incident_triage.adapters.source.ecr import (
    EcrLabelReader,
    make_ecr_client,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.git import (
    GitSourceProvider,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import (
    SourceLocator,
    SourceProvider,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.local import (
    FixtureLocator,
    LocalSourceProvider,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.locator import ChainLocator
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import (
    Fixture,
    load_fixture,
)
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

    With ``APP_ADAPTERS=real`` no fixture is loaded, and ``fixture_id`` is not used.

    Raises NotImplementedError when the settings select a real adapter that is not
    built yet; the message names each such adapter and its task, as in ``llm
    (Task 31)``. In simulated mode, raises ValueError without a ``fixture_id`` and
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
    # The adapters that are not built yet raise above: from here the model is recorded
    # and every sink is log.
    if settings.adapters == "real":
        fixture = None
    elif fixture_id is None:
        raise ValueError(
            "simulated adapters (APP_ADAPTERS=simulated) serve an incident fixture, but "
            "there is no fixture ID: set raw.fixture_id in the message to the ID of a "
            f"fixture in {settings.fixtures_dir / 'incidents'}"
        )
    else:
        fixture = load_fixture(fixture_id, root=settings.fixtures_dir)
    calls = CallLog.for_run(settings, run_id)
    return Adapters(
        k8s=_build_k8s(settings, fixture, calls),
        datadog=_build_datadog(settings, fixture, calls),
        locator=_build_locator(settings, fixture, calls),
        source=_build_source(settings, calls),
        llm=RecordedLlm(settings.fixtures_dir / "recorded", calls),
        sinks=tuple(LogSink(calls) for _ in settings.result_sinks),
    )


def _build_k8s(settings: Settings, fixture: Fixture | None, calls: CallLog) -> K8sAdapter:
    """``RealK8s`` with ``APP_ADAPTERS=real``, else the fake that serves ``fixture``.

    Building ``RealK8s`` touches no network: it loads its config on its first call.
    """
    if settings.adapters == "real":
        return RealK8s()
    if fixture is None:
        raise ValueError("the simulated Kubernetes adapter needs an incident fixture")
    return FakeK8s.from_fixture(fixture.k8s, calls)


def _build_datadog(settings: Settings, fixture: Fixture | None, calls: CallLog) -> DatadogAdapter:
    """``RealDatadog`` with ``APP_ADAPTERS=real``, else the fake that serves ``fixture``.

    Building ``RealDatadog`` touches no network: each call opens its own client.
    """
    if settings.adapters == "real":
        return RealDatadog(settings)
    if fixture is None:
        raise ValueError("the simulated Datadog adapter needs an incident fixture")
    return FakeDatadog.from_fixture(
        fixture.datadog, delay_s=settings.fake_datadog_delay_s, calls=calls
    )


def _build_locator(settings: Settings, fixture: Fixture | None, calls: CallLog) -> SourceLocator:
    """``ChainLocator`` with ``APP_ADAPTERS=real``, else the locator of ``fixture``'s source.

    Building ``ChainLocator`` touches no network: its ECR reader builds the boto3 client
    when it first reads an ECR image's labels.
    """
    if settings.adapters == "real":
        return ChainLocator(
            annotation_keys=(
                settings.source_annotation_repo_key,
                settings.source_annotation_commit_key,
            ),
            allowed_hosts=settings.git_allowed_hosts,
            ecr=EcrLabelReader(functools.partial(make_ecr_client, settings.aws_region)),
            config_map=settings.source_map_file,
        )
    if fixture is None:
        raise ValueError("the simulated source locator needs an incident fixture")
    return FixtureLocator.from_fixture(fixture.source, calls)


def _build_source(settings: Settings, calls: CallLog) -> SourceProvider:
    """``GitSourceProvider`` with ``APP_ADAPTERS=real``, else the fixture apps' provider.

    Building ``GitSourceProvider`` starts no git process and touches no file. It never
    accepts ``file://`` URLs here: that flag is for tests.
    """
    if settings.adapters == "real":
        return GitSourceProvider(
            settings.source_cache_dir,
            settings.git_token,
            settings.source_cache_max_mb,
            allowed_hosts=settings.git_allowed_hosts,
            # A run lasts at most the run deadline, so it never loses its checkout.
            min_age_s=settings.run_deadline_seconds,
        )
    return LocalSourceProvider(settings.fixtures_dir / "apps", calls)


def _real_adapters(settings: Settings) -> list[str]:
    """The adapters that the settings select in their real form, by name."""
    names = ["k8s", "datadog", "locator", "source"] if settings.adapters == "real" else []
    if settings.llm_mode == "bedrock":
        names.append("llm")
    names += [f"sink.{sink}" for sink in settings.result_sinks if sink != "log"]
    return names
