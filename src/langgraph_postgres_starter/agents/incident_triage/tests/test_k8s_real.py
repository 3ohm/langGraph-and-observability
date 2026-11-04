"""The real Kubernetes adapter, against mocked kubernetes_asyncio API classes (spec §9.3).

``_Cluster`` stands in for the API server: the tests swap ``CoreV1Api``, ``AppsV1Api``
and ``BatchV1Api`` for classes that serve its objects, and the config loaders for
functions that record their calls. The objects are the client's own models, so the
adapter maps what the client really returns. Nothing touches the network.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Self

import pytest
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from langgraph_postgres_starter.agents.incident_triage.adapters import _build_k8s, build_adapters
from langgraph_postgres_starter.agents.incident_triage.adapters.call_log import NO_CALL_LOG
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.fake import FakeK8s
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.interface import (
    ContainerStatus,
    K8sEvent,
    NodeCondition,
    PodInfo,
    Terminated,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.real import RealK8s
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_postgres_starter.agents.incident_triage.schemas import WorkloadRef
from langgraph_postgres_starter.policies import is_transient
from langgraph_postgres_starter.settings import load_settings

NS = "shop"
POD = "orders-7d9f8b6c5d-x2kqp"
RS = "orders-7d9f8b6c5d"
ORDERS = WorkloadRef(kind="Deployment", name="orders")
DIGEST = "sha256:" + "5" * 64
SEEN = datetime(2026, 11, 2, 10, 4, 55, tzinfo=UTC)


def _missing() -> ApiException:
    return ApiException(status=404, reason="Not Found")


@dataclass
class _LogResponse:
    """What read_namespaced_pod_log returns with _preload_content=False."""

    status: int
    body: bytes = b""
    reason: str = "OK"

    async def read(self) -> bytes:
        return self.body

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


@dataclass
class _Cluster:
    """The objects that the mocked API classes serve, and the calls they received."""

    pods: list[Any] = field(default_factory=list)
    replica_sets: dict[str, Any] = field(default_factory=dict)
    workloads: dict[tuple[str, str], Any] = field(default_factory=dict)
    events: list[Any] = field(default_factory=list)
    nodes: dict[str, Any] = field(default_factory=dict)
    log: _LogResponse | ApiException = field(default_factory=lambda: _LogResponse(200))
    # An error that every read of a pod raises, such as a 403 without RBAC.
    pod_error: ApiException | None = None
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> Self:
        cluster = self

        class Api:
            def __init__(self, api_client):
                assert isinstance(api_client, client.ApiClient)

        class Core(Api):
            async def read_namespaced_pod(self, name, namespace):
                cluster.calls.append(("read_namespaced_pod", {"name": name}))
                if cluster.pod_error is not None:
                    raise cluster.pod_error
                for pod in cluster.pods:
                    if (pod.metadata.namespace, pod.metadata.name) == (namespace, name):
                        return pod
                raise _missing()

            async def list_namespaced_pod(self, namespace, label_selector):
                cluster.calls.append(("list_namespaced_pod", {"label_selector": label_selector}))
                return client.V1PodList(
                    items=[p for p in cluster.pods if p.metadata.namespace == namespace]
                )

            async def list_namespaced_event(self, namespace, field_selector):
                cluster.calls.append(("list_namespaced_event", {"field_selector": field_selector}))
                return client.CoreV1EventList(items=cluster.events)

            async def read_namespaced_pod_log(self, name, namespace, **kwargs):
                cluster.calls.append(("read_namespaced_pod_log", {"name": name, **kwargs}))
                if isinstance(cluster.log, ApiException):
                    raise cluster.log
                return cluster.log

            async def read_node(self, name):
                if name not in cluster.nodes:
                    raise _missing()
                return cluster.nodes[name]

        def reader(kind: str) -> Callable[[Any, str, str], Awaitable[Any]]:
            async def read(self: Any, name: str, namespace: str) -> Any:
                cluster.calls.append((f"read {kind}", {"name": name}))
                if (kind, name) not in cluster.workloads:
                    raise _missing()
                return cluster.workloads[kind, name]

            return read

        class Apps(Api):
            read_namespaced_deployment = reader("Deployment")
            read_namespaced_stateful_set = reader("StatefulSet")
            read_namespaced_daemon_set = reader("DaemonSet")

            async def read_namespaced_replica_set(self, name, namespace):
                cluster.calls.append(("read_namespaced_replica_set", {"name": name}))
                if name not in cluster.replica_sets:
                    raise _missing()
                return cluster.replica_sets[name]

        class Batch(Api):
            read_namespaced_job = reader("Job")

        monkeypatch.setattr(client, "CoreV1Api", Core)
        monkeypatch.setattr(client, "AppsV1Api", Apps)
        monkeypatch.setattr(client, "BatchV1Api", Batch)
        return self


@pytest.fixture
def loads(monkeypatch):
    """Replace the config loaders. The list records which loader ran, in order."""
    calls = []

    def load_incluster_config(client_configuration=None, **kwargs):
        calls.append("incluster")
        client_configuration.host = "https://10.100.0.1:443"

    async def load_kube_config(client_configuration=None, **kwargs):
        calls.append("kubeconfig")
        client_configuration.host = "https://127.0.0.1:6443"

    monkeypatch.setattr(config, "load_incluster_config", load_incluster_config)
    monkeypatch.setattr(config, "load_kube_config", load_kube_config)
    return calls


def _owner(kind: str, name: str) -> client.V1OwnerReference:
    return client.V1OwnerReference(
        api_version="apps/v1", kind=kind, name=name, uid=f"uid-{name}", controller=True
    )


def _pod(
    name: str = POD,
    *,
    owner: client.V1OwnerReference | None = None,
    statuses: list[client.V1ContainerStatus] | None = None,
) -> client.V1Pod:
    return client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=name,
            namespace=NS,
            labels={"app": "orders"},
            annotations={"istio.io/rev": "default"},
            owner_references=[owner or _owner("ReplicaSet", RS)],
        ),
        spec=client.V1PodSpec(
            node_name="node-a",
            containers=[
                client.V1Container(name="orders", image="shop/orders:2.31.0"),
                client.V1Container(name="istio-proxy", image="istio/proxyv2:1.27.3"),
            ],
        ),
        status=client.V1PodStatus(phase="Running", container_statuses=statuses),
    )


def _crashing() -> client.V1ContainerStatus:
    return client.V1ContainerStatus(
        name="orders",
        image="docker.io/shop/orders:2.31.0",
        image_id=f"docker.io/shop/orders@{DIGEST}",
        ready=False,
        restart_count=6,
        state=client.V1ContainerState(
            waiting=client.V1ContainerStateWaiting(reason="CrashLoopBackOff")
        ),
        last_state=client.V1ContainerState(
            terminated=client.V1ContainerStateTerminated(exit_code=1, reason="Error")
        ),
    )


def _running_sidecar() -> client.V1ContainerStatus:
    return client.V1ContainerStatus(
        name="istio-proxy",
        image="docker.io/istio/proxyv2:1.27.3",
        image_id="",
        ready=True,
        restart_count=0,
        state=client.V1ContainerState(running=client.V1ContainerStateRunning()),
        last_state=client.V1ContainerState(),
    )


def _replica_set(name: str = RS, deployment: str | None = "orders") -> client.V1ReplicaSet:
    owners = [_owner("Deployment", deployment)] if deployment else None
    return client.V1ReplicaSet(
        metadata=client.V1ObjectMeta(name=name, namespace=NS, owner_references=owners),
        spec=client.V1ReplicaSetSpec(selector=client.V1LabelSelector()),
    )


def _deployment(
    *,
    match_labels: dict[str, str] | None = None,
    match_expressions: list[client.V1LabelSelectorRequirement] | None = None,
    annotations: dict[str, str] | None = None,
) -> client.V1Deployment:
    return client.V1Deployment(
        metadata=client.V1ObjectMeta(name="orders", namespace=NS, annotations=annotations),
        spec=client.V1DeploymentSpec(
            selector=client.V1LabelSelector(
                match_labels=match_labels, match_expressions=match_expressions
            ),
            template=client.V1PodTemplateSpec(),
        ),
    )


def _event(reason: str, **fields: Any) -> client.CoreV1Event:
    return client.CoreV1Event(
        metadata=client.V1ObjectMeta(name=f"{POD}.{reason}", namespace=NS),
        involved_object=client.V1ObjectReference(kind="Pod", name=POD),
        reason=reason,
        message=f"{reason} happened",
        type="Warning",
        **fields,
    )


async def test_real_previous_log_400_returns_none(monkeypatch, loads):
    # The API answers 400 when there is no previous container, as on a first crash.
    cluster = _Cluster(log=ApiException(status=400, reason="Bad Request")).install(monkeypatch)
    k8s = RealK8s()
    assert await k8s.read_logs(NS, POD, "orders", previous=True, tail_lines=500) is None

    # The same answer as a response that the client did not check.
    cluster.log = _LogResponse(400, b'{"kind":"Status","code":400}', reason="Bad Request")
    assert await k8s.read_logs(NS, POD, "orders", previous=True, tail_lines=500) is None

    # A server error goes up, and the graph's retry policy retries it.
    cluster.log = _LogResponse(503, reason="Service Unavailable")
    with pytest.raises(ApiException) as exc_info:
        await k8s.read_logs(NS, POD, "orders", previous=False, tail_lines=500)
    assert exc_info.value.status == 503
    assert is_transient(exc_info.value)


async def test_real_read_logs_asks_for_the_tail_and_keeps_raw_text(monkeypatch, loads):
    # A one-line JSON log stays as it is, and bytes that are not UTF-8 do not fail.
    body = b'{"level":"error","msg":"boom"}\n\xff\n'
    cluster = _Cluster(log=_LogResponse(200, body)).install(monkeypatch)

    text = await RealK8s().read_logs(NS, POD, "orders", previous=True, tail_lines=500)

    assert text == '{"level":"error","msg":"boom"}\n�\n'
    assert cluster.calls == [
        (
            "read_namespaced_pod_log",
            {
                "name": POD,
                "container": "orders",
                "previous": True,
                "tail_lines": 500,
                "_preload_content": False,
            },
        )
    ]


async def test_real_loads_config_lazily_with_fallback(monkeypatch, loads):
    cluster = _Cluster(pods=[_pod(statuses=[_crashing()])]).install(monkeypatch)
    cluster.replica_sets[RS] = _replica_set()

    k8s = RealK8s()
    assert loads == []  # the constructor loads nothing
    await k8s.get_pod(NS, POD)
    await k8s.get_pod(NS, POD)
    assert loads == ["incluster"]  # loaded once, on the first call

    # Outside a cluster, the in-cluster config fails and the kubeconfig is loaded.
    def no_cluster(client_configuration=None, **kwargs):
        loads.append("incluster")
        raise config.ConfigException("Service host/port is not set.")

    monkeypatch.setattr(config, "load_incluster_config", no_cluster)
    loads.clear()
    await RealK8s().get_pod(NS, POD)
    assert loads == ["incluster", "kubeconfig"]


async def test_real_get_pod_maps_status_and_owner_chain(monkeypatch, loads):
    cluster = _Cluster(pods=[_pod(statuses=[_crashing(), _running_sidecar()])])
    cluster.install(monkeypatch)
    cluster.replica_sets[RS] = _replica_set()
    k8s = RealK8s()

    pod = await k8s.get_pod(NS, POD)

    assert pod == PodInfo(
        name=POD,
        namespace=NS,
        node="node-a",
        phase="Running",
        containers=[
            ContainerStatus(
                name="orders",
                image="docker.io/shop/orders:2.31.0",
                image_id=f"docker.io/shop/orders@{DIGEST}",
                restart_count=6,
                state="waiting",
                waiting_reason="CrashLoopBackOff",
                last_terminated=Terminated(reason="Error", exit_code=1, signal=None),
                ready=False,
            ),
            ContainerStatus(
                name="istio-proxy",
                image="docker.io/istio/proxyv2:1.27.3",
                image_id=None,
                restart_count=0,
                state="running",
                waiting_reason=None,
                last_terminated=None,
                ready=True,
            ),
        ],
        annotations={"istio.io/rev": "default"},
        owner=ORDERS,  # Pod -> ReplicaSet -> Deployment
    )
    assert await k8s.get_owner_workload(NS, POD) == ORDERS
    assert await k8s.get_pod(NS, "gone") is None
    assert await k8s.get_owner_workload(NS, "gone") is None

    # A ReplicaSet without a Deployment, or one that is gone, gives no workload.
    cluster.replica_sets[RS] = _replica_set(deployment=None)
    assert await k8s.get_owner_workload(NS, POD) is None
    del cluster.replica_sets[RS]
    assert await k8s.get_owner_workload(NS, POD) is None


async def test_real_get_pod_maps_other_owners_and_states(monkeypatch, loads):
    # A failed Job pod that never restarts: its run ended, with no lastState.
    failed = client.V1ContainerStatus(
        name="orders",
        image="shop/orders:2.31.0",
        image_id="",
        ready=False,
        restart_count=0,
        state=client.V1ContainerState(
            terminated=client.V1ContainerStateTerminated(exit_code=1, reason="Error", signal=9)
        ),
    )
    job_pod = _pod("invoices-7xk2p", owner=_owner("Job", "invoices"), statuses=[failed])
    # A pod that waits to be scheduled has no container statuses yet.
    pending = _pod("batcher-0", owner=_owner("StatefulSet", "batcher"))
    pending.spec = client.V1PodSpec(containers=pending.spec.containers)  # no node_name
    pending.status.phase = "Pending"
    _Cluster(pods=[job_pod, pending]).install(monkeypatch)
    k8s = RealK8s()

    job = await k8s.get_pod(NS, "invoices-7xk2p")
    assert job is not None
    assert job.owner == WorkloadRef(kind="Job", name="invoices")
    assert job.containers[0].state == "terminated"
    assert job.containers[0].last_terminated == Terminated(reason="Error", exit_code=1, signal=9)

    waiting = await k8s.get_pod(NS, "batcher-0")
    assert waiting is not None
    assert (waiting.owner, waiting.node, waiting.phase) == (
        WorkloadRef(kind="StatefulSet", name="batcher"),
        None,
        "Pending",
    )
    assert waiting.containers[1] == ContainerStatus(
        name="istio-proxy",
        image="istio/proxyv2:1.27.3",
        image_id=None,
        restart_count=0,
        state="waiting",
        waiting_reason=None,
        last_terminated=None,
        ready=False,
    )


async def test_real_list_pods_for_workload_uses_selector_and_owner(monkeypatch, loads):
    other_rs = "orders-canary-5b7c9d8f6"
    cluster = _Cluster(
        pods=[
            _pod(statuses=[_crashing()]),
            # Matches the labels, but another Deployment owns it.
            _pod("orders-canary-5b7c9d8f6-q2w4z", owner=_owner("ReplicaSet", other_rs)),
            _pod("orders-7d9f8b6c5d-bcdfg", statuses=[_running_sidecar()]),
        ]
    ).install(monkeypatch)
    cluster.replica_sets |= {RS: _replica_set(), other_rs: _replica_set(other_rs, "orders-canary")}
    expressions = [
        client.V1LabelSelectorRequirement(key="tier", operator="In", values=["web", "api"]),
        client.V1LabelSelectorRequirement(key="track", operator="NotIn", values=["canary"]),
        client.V1LabelSelectorRequirement(key="team", operator="Exists"),
        client.V1LabelSelectorRequirement(key="legacy", operator="DoesNotExist"),
    ]
    cluster.workloads["Deployment", "orders"] = _deployment(
        match_labels={"app": "orders"}, match_expressions=expressions
    )
    k8s = RealK8s()

    pods = await k8s.list_pods_for_workload(NS, ORDERS)

    assert [pod.name for pod in pods] == [POD, "orders-7d9f8b6c5d-bcdfg"]
    selectors = [
        kw["label_selector"] for name, kw in cluster.calls if name == "list_namespaced_pod"
    ]
    assert selectors == ["app=orders,tier in (web,api),track notin (canary),team,!legacy"]
    # The pods of one ReplicaSet share one read of it.
    rs_reads = [kw["name"] for name, kw in cluster.calls if name == "read_namespaced_replica_set"]
    assert sorted(rs_reads) == sorted({RS, other_rs})
    # A workload that does not exist has no pods.
    assert await k8s.list_pods_for_workload(NS, WorkloadRef(kind="Job", name="gone")) == []


async def test_real_list_events_uses_field_selector(monkeypatch, loads):
    series_time = datetime(2026, 11, 2, 10, 5, 30, tzinfo=UTC)
    cluster = _Cluster(
        events=[
            _event("BackOff", count=20, last_timestamp=SEEN),
            # A newer event reports its time and count in a series.
            _event(
                "Unhealthy",
                event_time=SEEN,
                series=client.CoreV1EventSeries(count=4, last_observed_time=series_time),
            ),
            _event("Scheduled", event_time=SEEN),
        ]
    ).install(monkeypatch)

    events = await RealK8s().list_events(NS, "Pod", POD)

    assert cluster.calls == [
        (
            "list_namespaced_event",
            {"field_selector": f"involvedObject.kind=Pod,involvedObject.name={POD}"},
        )
    ]
    assert events == [
        K8sEvent(
            reason="BackOff",
            message="BackOff happened",
            type="Warning",
            count=20,
            last_seen=SEEN,
            involved_kind="Pod",
            involved_name=POD,
        ),
        K8sEvent(
            reason="Unhealthy",
            message="Unhealthy happened",
            type="Warning",
            count=4,
            last_seen=series_time,
            involved_kind="Pod",
            involved_name=POD,
        ),
        K8sEvent(
            reason="Scheduled",
            message="Scheduled happened",
            type="Warning",
            count=1,
            last_seen=SEEN,
            involved_kind="Pod",
            involved_name=POD,
        ),
    ]


async def test_real_node_conditions_and_workload_annotations(monkeypatch, loads):
    node = client.V1Node(
        status=client.V1NodeStatus(
            conditions=[
                client.V1NodeCondition(type="MemoryPressure", status="False", reason="OK"),
                client.V1NodeCondition(type="Ready", status="True"),
            ]
        )
    )
    cluster = _Cluster(nodes={"node-a": node}).install(monkeypatch)
    cluster.workloads["Deployment", "orders"] = _deployment(
        annotations={"deployment.kubernetes.io/revision": "41"}
    )
    k8s = RealK8s()

    assert await k8s.get_node_conditions("node-a") == [
        NodeCondition(type="MemoryPressure", status="False", reason="OK"),
        NodeCondition(type="Ready", status="True", reason=None),
    ]
    assert await k8s.get_node_conditions("gone") == []
    assert await k8s.get_workload_annotations(NS, ORDERS) == {
        "deployment.kubernetes.io/revision": "41"
    }
    gone = WorkloadRef(kind="DaemonSet", name="gone")
    assert await k8s.get_workload_annotations(NS, gone) == {}


def test_real_mode_builds_real_k8s_without_network(monkeypatch):
    def no_loading(*args, **kwargs):
        raise AssertionError("building the adapter must not load the config")

    monkeypatch.setattr(config, "load_incluster_config", no_loading)
    monkeypatch.setattr(config, "load_kube_config", no_loading)

    assert isinstance(_build_k8s(load_settings(adapters="real"), None, NO_CALL_LOG), RealK8s)
    fixture = load_fixture("python-keyerror")
    simulated = _build_k8s(load_settings(), fixture, NO_CALL_LOG)
    assert isinstance(simulated, FakeK8s)

    # Real mode builds every adapter it selects, without a fixture.
    assert isinstance(build_adapters(load_settings(adapters="real"), fixture_id=None).k8s, RealK8s)


async def test_real_other_errors_propagate_and_odd_owners(monkeypatch, loads):
    # A pod whose owner is not its controller has no workload.
    loose = _owner("ReplicaSet", RS)
    loose.controller = False
    cluster = _Cluster(pods=[_pod(owner=loose)]).install(monkeypatch)
    k8s = RealK8s()
    assert await k8s.get_owner_workload(NS, POD) is None

    # Only a 404 means "missing". A 403 (no RBAC) goes up, and the graph does not retry it.
    cluster.pod_error = ApiException(status=403, reason="Forbidden")
    with pytest.raises(ApiException) as exc_info:
        await k8s.get_pod(NS, POD)
    assert exc_info.value.status == 403
    assert not is_transient(exc_info.value)

    # StatefulSets are read from apps/v1, as Deployments and DaemonSets are.
    cluster.workloads["StatefulSet", "batcher"] = client.V1StatefulSet(
        metadata=client.V1ObjectMeta(name="batcher", annotations={"team": "warehouse"}),
        spec=client.V1StatefulSetSpec(
            selector=client.V1LabelSelector(match_labels={"app": "batcher"}),
            template=client.V1PodTemplateSpec(),
        ),
    )
    batcher = WorkloadRef(kind="StatefulSet", name="batcher")
    assert await k8s.get_workload_annotations(NS, batcher) == {"team": "warehouse"}
