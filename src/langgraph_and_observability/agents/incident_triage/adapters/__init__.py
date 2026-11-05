"""The agent's adapters: one interface per external system, each with a fake (spec §9, §10.2).

Each subpackage holds one interface (``interface.py``) and the adapters that implement
it. ``build_adapters`` picks them from the settings:

- ``APP_ADAPTERS`` picks Kubernetes, Datadog and the source adapters. ``simulated``
  serves an incident fixture (``fixtures/FORMAT.md``).
- ``APP_LLM_MODE`` picks the model. ``recorded`` replies with recorded responses;
  ``bedrock`` calls Claude on Amazon Bedrock (``llm/bedrock.py``).
- ``APP_RESULT_SINKS`` lists the sinks, by name. ``log`` is the same in every mode;
  ``sqs`` sends to ``APP_RESULT_QUEUE_URL`` (``sinks/sqs.py``).
"""

import functools
from dataclasses import dataclass
from uuid import UUID

from langgraph_and_observability.agents.incident_triage.adapters.call_log import CallLog
from langgraph_and_observability.agents.incident_triage.adapters.datadog.fake import FakeDatadog
from langgraph_and_observability.agents.incident_triage.adapters.datadog.interface import (
    DatadogAdapter,
)
from langgraph_and_observability.agents.incident_triage.adapters.datadog.real import RealDatadog
from langgraph_and_observability.agents.incident_triage.adapters.k8s.fake import FakeK8s
from langgraph_and_observability.agents.incident_triage.adapters.k8s.interface import K8sAdapter
from langgraph_and_observability.agents.incident_triage.adapters.k8s.real import RealK8s
from langgraph_and_observability.agents.incident_triage.adapters.llm.bedrock import BedrockLlm
from langgraph_and_observability.agents.incident_triage.adapters.llm.interface import LlmAdapter
from langgraph_and_observability.agents.incident_triage.adapters.llm.recorded import RecordedLlm
from langgraph_and_observability.agents.incident_triage.adapters.sinks.interface import ResultSink
from langgraph_and_observability.agents.incident_triage.adapters.sinks.log import LogSink
from langgraph_and_observability.agents.incident_triage.adapters.sinks.sqs import (
    SqsSink,
    make_sqs_client,
)
from langgraph_and_observability.agents.incident_triage.adapters.source.ecr import (
    EcrLabelReader,
    make_ecr_client,
)
from langgraph_and_observability.agents.incident_triage.adapters.source.git import (
    GitSourceProvider,
)
from langgraph_and_observability.agents.incident_triage.adapters.source.interface import (
    SourceLocator,
    SourceProvider,
)
from langgraph_and_observability.agents.incident_triage.adapters.source.local import (
    FixtureLocator,
    LocalSourceProvider,
)
from langgraph_and_observability.agents.incident_triage.adapters.source.locator import ChainLocator
from langgraph_and_observability.agents.incident_triage.fixtures.loader import (
    Fixture,
    load_fixture,
)
from langgraph_and_observability.settings import Settings


@dataclass(frozen=True)
class Adapters:
    """The adapters of one run. ``sinks`` follow the order of ``APP_RESULT_SINKS``."""

    k8s: K8sAdapter
    datadog: DatadogAdapter
    locator: SourceLocator
    source: SourceProvider
    llm: LlmAdapter
    sinks: tuple[ResultSink, ...]


def build_adapters(
    settings: Settings, *, fixture_id: str | None, run_id: UUID | None = None
) -> Adapters:
    """Build a run's adapters from ``settings``.

    In simulated mode the fakes serve the incident fixture ``fixture_id`` from
    ``settings.fixtures_dir``. With ``settings.fake_call_log_dir`` and ``run_id`` set,
    every adapter appends its calls to the run's call log (``call_log.py``).
    Constructing an adapter touches no network.

    With ``APP_ADAPTERS=real`` no fixture is loaded, and ``fixture_id`` is not used.

    Raises ValueError for a sink that ``_build_sink`` cannot build. In simulated mode,
    raises ValueError without a ``fixture_id`` and FileNotFoundError when the fixture is
    missing.
    """
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
        llm=_build_llm(settings, calls),
        sinks=tuple(_build_sink(name, settings, calls) for name in settings.result_sinks),
    )


def _build_sink(name: str, settings: Settings, calls: CallLog) -> ResultSink:
    """The sink named ``name`` in ``APP_RESULT_SINKS``: ``log`` or ``sqs``.

    Raises ValueError for ``sqs`` without ``APP_RESULT_QUEUE_URL``, and for any other
    name. Building ``SqsSink`` touches no network: it builds its boto3 client on its
    first send.
    """
    if name == "log":
        return LogSink(calls)
    if name == "sqs":
        if not settings.result_queue_url:
            raise ValueError(
                "the sqs result sink (APP_RESULT_SINKS) sends to APP_RESULT_QUEUE_URL, "
                "which is not set"
            )
        return SqsSink(
            functools.partial(make_sqs_client, settings.aws_region, settings.sqs_endpoint_url),
            settings.result_queue_url,
        )
    raise ValueError(f"unknown result sink {name!r} in APP_RESULT_SINKS: the sinks are log, sqs")


def _build_llm(settings: Settings, calls: CallLog) -> LlmAdapter:
    """``BedrockLlm`` with ``APP_LLM_MODE=bedrock``, else the recorded model.

    Building ``BedrockLlm`` touches no network: it builds its boto3 client on its first
    call.
    """
    if settings.llm_mode == "bedrock":
        return BedrockLlm(settings)
    return RecordedLlm(settings.fixtures_dir / "recorded", calls)


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
