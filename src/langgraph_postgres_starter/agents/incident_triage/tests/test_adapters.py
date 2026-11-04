"""The adapters, their fakes, the run context and the fixture loader (spec §9, §10.2, §10.3).

Each test writes a small fixture, ``_sample``, under ``tmp_path`` and points the
settings (``fixtures_dir``) or the loader (``root=``) at it, so no test depends on the
agent's own fixtures.
"""

import asyncio
import json
import re
import socket
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import pydantic
import pytest
import structlog
from langgraph.runtime import Runtime

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.agents.incident_triage.adapters import build_adapters
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.interface import DdEvent
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.fake import FakeK8s
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.interface import (
    ContainerStatus,
    K8sEvent,
    NodeCondition,
    PodInfo,
    Terminated,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmResponse,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.llm.recorded import RecordedLlm
from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.log import LogSink
from langgraph_postgres_starter.agents.incident_triage.adapters.source.git import (
    GitSourceProvider,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import SourceRef
from langgraph_postgres_starter.agents.incident_triage.adapters.source.local import (
    LocalSourceProvider,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.locator import ChainLocator
from langgraph_postgres_starter.agents.incident_triage.context import (
    ctx,
    default_context,
    make_context,
)
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import (
    list_fixtures,
    load_fixture,
)
from langgraph_postgres_starter.agents.incident_triage.schemas import (
    Assessment,
    ImageRef,
    IncidentMessage,
    Target,
    TriageResult,
    Usage,
    WorkloadRef,
)
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.settings import get_settings, load_settings

SAMPLE = "_sample"
NS = "shop"
POD = "orders-7d9f-x2"
ORDERS = WorkloadRef(kind="Deployment", name="orders")
DIGEST = "sha256:" + "a" * 64

INCIDENT = {
    "version": 1,
    "source": "manual",
    "namespace": NS,
    "pod": POD,
    "workload": {"kind": "Deployment", "name": "orders"},
    "container": "orders",
    "raw": {"fixture_id": SAMPLE},
}
K8S = {
    "pods": [
        {
            "name": POD,
            "namespace": NS,
            "node": "node-a",
            "phase": "Running",
            "containers": [
                {
                    "name": "orders",
                    "image": "registry.example.com/shop/orders:1.4.2",
                    "image_id": f"registry.example.com/shop/orders@{DIGEST}",
                    "restart_count": 4,
                    "state": "waiting",
                    "waiting_reason": "CrashLoopBackOff",
                    "last_terminated": {"reason": "Error", "exit_code": 1, "signal": None},
                    "ready": False,
                },
                {
                    "name": "istio-proxy",
                    "image": "istio/proxyv2:1.27.0",
                    "image_id": None,
                    "restart_count": 0,
                    "state": "running",
                    "waiting_reason": None,
                    "last_terminated": None,
                    "ready": True,
                },
            ],
            "annotations": {"sidecar.istio.io/status": "injected"},
            "owner": {"kind": "Deployment", "name": "orders"},
        }
    ],
    "events": [
        {
            "reason": "BackOff",
            "message": "Back-off restarting failed container orders",
            "type": "Warning",
            "count": 12,
            "last_seen": "2026-11-02T10:01:00Z",
            "involved_kind": "Pod",
            "involved_name": POD,
        },
        {
            "reason": "ScalingReplicaSet",
            "message": "Scaled up replica set orders-7d9f to 1",
            "type": "Normal",
            "count": 1,
            "last_seen": None,
            "involved_kind": "Deployment",
            "involved_name": "orders",
        },
    ],
    "logs": {
        f"{POD}/orders/current": "starting orders\n",
        f"{POD}/orders/previous": "starting orders\nprocessing order 42\nKeyError: 'customer'\n",
        f"{POD}/istio-proxy/previous": None,
    },
    "nodes": {
        "node-a": [
            {"type": "Ready", "status": "True", "reason": "KubeletReady"},
            {"type": "MemoryPressure", "status": "False", "reason": None},
        ]
    },
    "annotations": {"lps.dev/source-repo": "https://github.com/example/orders"},
}
DATADOG = {
    "events": [
        {
            "id": "7771234567890",
            "title": "orders restarted",
            "text": "Container orders restarted 4 times",
            "timestamp": "2026-11-02T10:00:30Z",
            "tags": ["kube_namespace:shop", f"pod_name:{POD}"],
            "source": "kubernetes",
        },
        {
            "id": "7771234567891",
            "title": "Monitor: orders error rate",
            "text": "Error rate above 5%",
            "timestamp": "2026-11-02T10:01:30Z",
            "tags": ["kube_deployment:orders"],
            "source": None,
        },
    ]
}
EXPECTED = {
    "category": "application",
    "code": {"path": "main.py", "start_line": 1, "end_line": 2, "symbol": "handle"},
    "facts": ["KeyError: 'customer'"],
}
SOURCE = {"app": "sample-app", "commit": "fixture"}
SAMPLE_FILES: dict[str, dict[str, Any]] = {
    "incident.json": INCIDENT,
    "k8s.json": K8S,
    "datadog.json": DATADOG,
    "expected.json": EXPECTED,
    "source.json": SOURCE,
}
REPLY = {
    "text": '{"category": "application"}',
    "stop_reason": "end_turn",
    "model_id": "anthropic.claude-opus-5-5",
    "usage": {"input_tokens": 1200, "output_tokens": 90, "cache_read_input_tokens": 800},
}

TARGET = Target(
    cluster="local",
    namespace=NS,
    workload=ORDERS,
    pod=POD,
    container="orders",
    node="node-a",
    images=[
        ImageRef(container="orders", image="registry.example.com/shop/orders:1.4.2", digest=DIGEST)
    ],
)
RESULT = TriageResult(
    run_id=new_id(),
    dedup_key="0" * 64,
    target=TARGET,
    assessment=Assessment(
        collect_code=True,
        infra_signals=[],
        app_signals=["stack trace"],
        reasons=["A stack trace with no infrastructure signal: collect code."],
    ),
    analysis=None,
    refusal=False,
    model_id=None,
    usage=None,
    evidence_counts={"k8s": 3},
    created_at=datetime(2026, 11, 2, 10, 32, tzinfo=UTC),
)
WINDOW = (datetime(2026, 11, 2, 9, 30, tzinfo=UTC), datetime(2026, 11, 2, 10, 5, tzinfo=UTC))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _write_incident(root: Path, fixture_id: str, files: dict[str, dict[str, Any]]) -> None:
    for name, value in files.items():
        _write_json(root / "incidents" / fixture_id / name, value)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A fixtures directory with the ``_sample`` incident, its app and one recorded reply."""
    _write_incident(tmp_path, SAMPLE, SAMPLE_FILES)
    app = tmp_path / "apps" / "sample-app"
    app.mkdir(parents=True)
    (app / "main.py").write_text("def handle(order):\n    return order['customer']\n")
    _write_json(tmp_path / "recorded" / SAMPLE / "v1" / "attempt-0.json", REPLY)
    return tmp_path


def _settings(root: Path, **overrides: Any):
    return load_settings(fixtures_dir=root, **overrides)


def _request(fixture_id: str | None = SAMPLE, prompt_version: str = "v1", attempt: int = 0):
    return AnalysisRequest(
        system="You triage Kubernetes incidents.",
        user="<evidence>",
        schema={"type": "object"},
        fixture_id=fixture_id,
        attempt=attempt,
        prompt_version=prompt_version,
    )


async def test_fake_k8s_returns_fixture_pod(root):
    k8s = build_adapters(_settings(root), fixture_id=SAMPLE).k8s

    pod = await k8s.get_pod(NS, POD)

    assert pod == PodInfo(
        name=POD,
        namespace=NS,
        node="node-a",
        phase="Running",
        containers=[
            ContainerStatus(
                name="orders",
                image="registry.example.com/shop/orders:1.4.2",
                image_id=f"registry.example.com/shop/orders@{DIGEST}",
                restart_count=4,
                state="waiting",
                waiting_reason="CrashLoopBackOff",
                last_terminated=Terminated(reason="Error", exit_code=1, signal=None),
                ready=False,
            ),
            ContainerStatus(
                name="istio-proxy",
                image="istio/proxyv2:1.27.0",
                image_id=None,
                restart_count=0,
                state="running",
                waiting_reason=None,
                last_terminated=None,
                ready=True,
            ),
        ],
        annotations={"sidecar.istio.io/status": "injected"},
        owner=ORDERS,
    )
    assert await k8s.get_pod("billing", POD) is None
    assert await k8s.get_pod(NS, "orders-gone") is None
    assert await k8s.list_pods_for_workload(NS, ORDERS) == [pod]
    assert await k8s.list_pods_for_workload(NS, WorkloadRef(kind="Deployment", name="cart")) == []
    assert await k8s.get_owner_workload(NS, POD) == ORDERS
    assert await k8s.get_owner_workload(NS, "orders-gone") is None


async def test_fake_k8s_serves_events_logs_nodes_and_annotations(root):
    k8s = build_adapters(_settings(root), fixture_id=SAMPLE).k8s

    assert await k8s.list_events(NS, "Pod", POD) == [
        K8sEvent(
            reason="BackOff",
            message="Back-off restarting failed container orders",
            type="Warning",
            count=12,
            last_seen=datetime(2026, 11, 2, 10, 1, tzinfo=UTC),
            involved_kind="Pod",
            involved_name=POD,
        )
    ]
    assert [e.reason for e in await k8s.list_events(NS, "Deployment", "orders")] == [
        "ScalingReplicaSet"
    ]
    assert await k8s.list_events(NS, "Pod", "orders-gone") == []

    # The last tail_lines lines, as the Kubernetes API returns them.
    previous = await k8s.read_logs(NS, POD, "orders", previous=True, tail_lines=2)
    assert previous == "processing order 42\nKeyError: 'customer'\n"
    current = await k8s.read_logs(NS, POD, "orders", previous=False, tail_lines=500)
    assert current == "starting orders\n"
    assert await k8s.read_logs(NS, POD, "orders", previous=True, tail_lines=0) == ""
    # A null log, a missing log and a pod in another namespace have no log.
    assert await k8s.read_logs(NS, POD, "istio-proxy", previous=True, tail_lines=500) is None
    assert await k8s.read_logs(NS, POD, "istio-proxy", previous=False, tail_lines=500) is None
    assert await k8s.read_logs("billing", POD, "orders", previous=True, tail_lines=500) is None

    assert await k8s.get_node_conditions("node-a") == [
        NodeCondition(type="Ready", status="True", reason="KubeletReady"),
        NodeCondition(type="MemoryPressure", status="False", reason=None),
    ]
    assert await k8s.get_node_conditions("node-b") == []
    assert await k8s.get_workload_annotations(NS, ORDERS) == {
        "lps.dev/source-repo": "https://github.com/example/orders"
    }


@pytest.mark.parametrize(
    "change",
    [
        lambda k8s: k8s["pods"][0]["containers"][0].update(state="CrashLoopBackOff"),
        lambda k8s: k8s["pods"][0]["containers"][0].update(restarts=4),
        lambda k8s: k8s["events"][0].pop("involved_name"),
        lambda k8s: k8s.pop("nodes"),
    ],
    ids=["bad-state", "unknown-key", "missing-key", "missing-section"],
)
def test_fake_k8s_rejects_a_malformed_fixture(change):
    k8s = json.loads(json.dumps(K8S))
    change(k8s)

    with pytest.raises(pydantic.ValidationError):
        FakeK8s.from_fixture(k8s)


async def test_fake_datadog_waits_then_returns_events(root):
    run_id = new_id()
    settings = _settings(root, fake_datadog_delay_s=0.05, fake_call_log_dir=root / "calls")
    datadog = build_adapters(settings, fixture_id=SAMPLE, run_id=run_id).datadog
    call_log = root / "calls" / f"{run_id}.log"

    search = asyncio.create_task(datadog.search_events("kube_namespace:shop", *WINDOW, 50))
    await asyncio.sleep(0)  # the search starts, then waits for the delay

    assert call_log.read_text().splitlines() == ["datadog.search_events:start"]
    events = await search
    assert call_log.read_text().splitlines() == [
        "datadog.search_events:start",
        "datadog.search_events",
    ]
    assert events[0] == DdEvent(
        id="7771234567890",
        title="orders restarted",
        text="Container orders restarted 4 times",
        timestamp=datetime(2026, 11, 2, 10, 0, 30, tzinfo=UTC),
        tags=["kube_namespace:shop", f"pod_name:{POD}"],
        source="kubernetes",
    )
    assert [e.id for e in events] == ["7771234567890", "7771234567891"]
    assert len(await datadog.search_events("kube_namespace:shop", *WINDOW, 1)) == 1


async def test_fake_call_log_written(root):
    call_dir = root / "calls" / "nested"  # build_adapters does not need it to exist
    run_id = new_id()
    adapters = build_adapters(
        _settings(root, fake_call_log_dir=call_dir), fixture_id=SAMPLE, run_id=run_id
    )

    await adapters.k8s.get_pod(NS, POD)
    await adapters.k8s.read_logs(NS, POD, "orders", previous=True, tail_lines=500)
    await adapters.datadog.search_events("kube_namespace:shop", *WINDOW, 50)
    ref = await adapters.locator.locate(TARGET, {})
    assert ref is not None
    await adapters.source.checkout(ref)
    await adapters.llm.analyze(_request())
    [sink] = adapters.sinks
    await sink.send(RESULT, f"publish:{RESULT.run_id}:log")

    assert (call_dir / f"{run_id}.log").read_text().splitlines() == [
        "k8s.get_pod",
        "k8s.read_logs",
        "datadog.search_events:start",
        "datadog.search_events",
        "locator.locate",
        "source.checkout",
        "llm.analyze",
        "sink.log.send",
    ]

    # Without a run ID, the adapters write no call log.
    no_run = build_adapters(_settings(root, fake_call_log_dir=call_dir), fixture_id=SAMPLE)
    await no_run.k8s.get_pod(NS, POD)
    assert list(call_dir.iterdir()) == [call_dir / f"{run_id}.log"]


async def test_recorded_llm_returns_recorded_reply(root):
    _write_json(
        root / "recorded" / "_test" / "invalid-json" / "v1" / "attempt-1.json",
        REPLY | {"text": "not json {"},
    )
    llm = build_adapters(_settings(root), fixture_id=SAMPLE).llm

    assert await llm.analyze(_request()) == LlmResponse(
        text='{"category": "application"}',
        stop_reason="end_turn",
        model_id="anthropic.claude-opus-5-5",
        usage=Usage(input_tokens=1200, output_tokens=90, cache_read_input_tokens=800),
    )
    # A fixture ID may name a folder below recorded/.
    reply = await llm.analyze(_request(fixture_id="_test/invalid-json", attempt=1))
    assert reply.text == "not json {"


async def test_recorded_llm_missing_reply_raises_clear_error(root):
    llm = RecordedLlm(root / "recorded")
    expected = root / "recorded" / SAMPLE / "v9" / "attempt-3.json"

    with pytest.raises(FileNotFoundError) as exc_info:
        await llm.analyze(_request(prompt_version="v9", attempt=3))
    assert str(expected) in str(exc_info.value)

    with pytest.raises(ValueError, match="fixture"):
        await llm.analyze(_request(fixture_id=None))
    with pytest.raises(ValueError, match="inside"):
        await llm.analyze(_request(fixture_id="../../etc"))


async def test_recorded_llm_malformed_reply_names_its_file(root):
    path = root / "recorded" / SAMPLE / "v1" / "attempt-1.json"
    _write_json(path, REPLY | {"stopReason": "end_turn"})

    with pytest.raises(ValueError) as exc_info:
        await RecordedLlm(root / "recorded").analyze(_request(attempt=1))
    assert str(path) in str(exc_info.value)


# Each task that adds a real adapter removes its row here (ruling R21).
@pytest.mark.parametrize(
    ("overrides", "adapter", "task"),
    [
        ({"llm_mode": "bedrock"}, "llm", 31),
        ({"result_sinks": ["log", "sqs"]}, "sink.sqs", 32),
    ],
    ids=["llm", "sink.sqs"],
)
def test_real_mode_not_ready_names_task(overrides, adapter, task):
    with pytest.raises(NotImplementedError) as exc_info:
        build_adapters(load_settings(**overrides), fixture_id=SAMPLE)
    assert f"{adapter} (Task {task})" in str(exc_info.value)


async def test_real_mode_builds_source_adapters_without_network(monkeypatch, tmp_path):
    def no_network(*args, **kwargs):
        raise AssertionError("building the adapters must not touch the network or run git")

    monkeypatch.setattr(socket, "getaddrinfo", no_network)
    monkeypatch.setattr(socket.socket, "connect", no_network)
    # boto3 looks for credentials, which can mean a network call, when it builds a client.
    monkeypatch.setattr(boto3.session, "Session", no_network)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", no_network)
    settings = load_settings(
        adapters="real",
        source_cache_dir=tmp_path / "cache",
        source_map_file=tmp_path / "source-map.yaml",
        git_token="token-for-tests",  # noqa: S106 - a fake token
        git_allowed_hosts=["git.example.com"],
    )

    adapters = build_adapters(settings, fixture_id=None)

    assert isinstance(adapters.locator, ChainLocator)
    assert isinstance(adapters.source, GitSourceProvider)
    # Nothing is read or written before the first call either.
    assert not (tmp_path / "cache").exists()
    # The real provider takes only https:// URLs on APP_GIT_ALLOWED_HOSTS (ruling R59),
    # and refuses the others before git runs (create_subprocess_exec would raise).
    for repo_url, error in [
        (f"file://{tmp_path}/orders.git", "https://"),
        ("https://github.com/example/orders", "'github.com' is not in"),
    ]:
        with pytest.raises(ValueError, match=error):
            await adapters.source.checkout(SourceRef(repo_url, "main", "annotation"))


def test_simulated_mode_needs_a_fixture(root):
    with pytest.raises(ValueError, match="fixture"):
        build_adapters(_settings(root), fixture_id=None)
    with pytest.raises(FileNotFoundError, match=re.escape(str(root / "incidents" / "cart-oom"))):
        build_adapters(_settings(root), fixture_id="cart-oom")


def test_load_fixture_reads_every_file(root):
    fixture = load_fixture(SAMPLE, root=root)

    assert fixture.id == SAMPLE
    assert (fixture.incident, fixture.k8s, fixture.datadog) == (INCIDENT, K8S, DATADOG)
    assert (fixture.expected, fixture.source) == (EXPECTED, SOURCE)
    IncidentMessage.model_validate(fixture.incident)


def test_load_fixture_optional_and_required_files(root):
    without_source = {k: v for k, v in SAMPLE_FILES.items() if k != "source.json"}
    _write_incident(root, "infra-oom", without_source)
    _write_incident(
        root, "_test/no-k8s", {k: v for k, v in without_source.items() if k != "k8s.json"}
    )

    assert load_fixture("infra-oom", root=root).source is None
    with pytest.raises(FileNotFoundError) as exc_info:
        load_fixture("_test/no-k8s", root=root)
    assert str(root / "incidents" / "_test" / "no-k8s" / "k8s.json") in str(exc_info.value)
    with pytest.raises(FileNotFoundError, match="missing-fixture"):
        load_fixture("missing-fixture", root=root)
    with pytest.raises(ValueError, match="inside"):
        load_fixture("../recorded", root=root)


@pytest.mark.parametrize(
    ("content", "error"), [('{"pods": [', "not valid JSON"), ("[]", "JSON object")]
)
def test_load_fixture_names_a_malformed_file(root, content, error):
    path = root / "incidents" / SAMPLE / "datadog.json"
    path.write_text(content)

    with pytest.raises(ValueError, match=error) as exc_info:
        load_fixture(SAMPLE, root=root)
    assert str(path) in str(exc_info.value)


def test_list_fixtures_sorted_without_test_fixtures(root):
    for fixture_id in ("zeta-crash", "alpha-crash", "_test/invalid-json"):
        _write_incident(root, fixture_id, SAMPLE_FILES)

    assert list_fixtures(root) == ["alpha-crash", "zeta-crash"]


async def test_fixture_locator_and_local_source_provider(root):
    _write_incident(
        root, "infra-oom", {k: v for k, v in SAMPLE_FILES.items() if k != "source.json"}
    )
    adapters = build_adapters(_settings(root), fixture_id=SAMPLE)

    ref = await adapters.locator.locate(TARGET, {})

    assert ref == SourceRef(repo_url="fixture://sample-app", commit="fixture", origin="fixture")
    assert await adapters.source.checkout(ref) == root / "apps" / "sample-app"
    no_source = build_adapters(_settings(root), fixture_id="infra-oom")
    assert await no_source.locator.locate(TARGET, {}) is None


@pytest.mark.parametrize(
    "repo_url",
    [
        "https://github.com/example/orders",
        "fixture://../incidents",
        "fixture://sample-app/../../recorded",
        "fixture:///etc",
        "fixture://",
    ],
)
async def test_local_source_provider_rejects_other_sources(root, repo_url):
    provider = LocalSourceProvider(root / "apps")

    with pytest.raises(ValueError, match=r"fixture://|inside"):
        await provider.checkout(SourceRef(repo_url=repo_url, commit="fixture", origin="fixture"))


async def test_local_source_provider_missing_app(root):
    ref = SourceRef(repo_url="fixture://cart", commit="fixture", origin="fixture")

    with pytest.raises(FileNotFoundError, match=re.escape(str(root / "apps" / "cart"))):
        await LocalSourceProvider(root / "apps").checkout(ref)


async def test_log_sink_writes_one_published_event():
    key = f"publish:{RESULT.run_id}:log"

    with structlog.testing.capture_logs() as logs:
        receipt = await LogSink().send(RESULT, key)

    assert receipt == {"sink": "log"}
    assert logs == [
        {
            "event": "triage.published",
            "log_level": "info",
            "result": RESULT.model_dump(mode="json"),
            "idempotency_key": key,
        }
    ]
    assert LogSink().name == "log"


async def test_make_context_takes_fixture_from_message(root):
    settings = _settings(root, fake_call_log_dir=root / "calls")
    info = RunInfo(run_id=new_id(), dedup_key="0" * 64, attempt=2)
    message = IncidentMessage.model_validate(INCIDENT)

    context = make_context(info, message, settings)

    assert (context.run, context.settings, context.fixture_id) == (info, settings, SAMPLE)
    assert await context.adapters.k8s.get_pod(NS, POD) is not None
    # The adapters log their calls under the run's ID.
    assert (root / "calls" / f"{info.run_id}.log").read_text() == "k8s.get_pod\n"
    # Simulated adapters need the message's raw.fixture_id.
    for raw in (None, {}, {"fixture_id": 7}):
        with pytest.raises(ValueError, match="fixture"):
            make_context(info, message.model_copy(update={"raw": raw}), settings)


async def test_default_context_uses_studio_fixture(root, monkeypatch):
    monkeypatch.setenv("APP_FIXTURES_DIR", str(root))
    monkeypatch.setenv("APP_STUDIO_FIXTURE", SAMPLE)

    context = default_context()

    assert context.fixture_id == SAMPLE
    assert context.settings is get_settings()
    assert (context.run.dedup_key, context.run.attempt) == ("studio", 1)
    assert context.run.run_id.version == 7
    assert await context.adapters.k8s.get_pod(NS, POD) is not None
    assert default_context().run.run_id != context.run.run_id

    # ctx returns the run's context, or the default one when the run has none.
    assert ctx(Runtime(context=context)) is context
    assert ctx(Runtime()).fixture_id == SAMPLE
