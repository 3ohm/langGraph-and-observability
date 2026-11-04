"""The resolve_target step: from the message to the pod, workload and container (spec §9.3).

The tests use the agent's incident fixtures through the fake adapter, and small in-test
pods for the edge cases (Review Focus 1 and 2).
"""

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import pytest
from langgraph.runtime import Runtime

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.agents.incident_triage.adapters import build_adapters
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.fake import FakeK8s
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.interface import (
    ContainerStatus,
    PodInfo,
    Terminated,
)
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_postgres_starter.agents.incident_triage.nodes.k8s import collect_k8s
from langgraph_postgres_starter.agents.incident_triage.nodes.resolve import (
    _deployment_of,
    resolve_target,
)
from langgraph_postgres_starter.agents.incident_triage.schemas import (
    ImageRef,
    IncidentMessage,
    Target,
    WorkloadRef,
)
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.settings import load_settings

ORDERS = WorkloadRef(kind="Deployment", name="orders")
ORDERS_POD = "orders-7d9f8b6c5d-x2kqp"
ORDERS_NODE = "ip-10-0-12-34.eu-west-1.compute.internal"


def _runtime(
    fixture_id: str = "python-keyerror", *, k8s: FakeK8s | None = None, **overrides: Any
) -> Runtime[TriageContext]:
    """A runtime whose adapters serve ``fixture_id``, or ``k8s`` for Kubernetes if given."""
    settings = load_settings(**overrides)
    adapters = build_adapters(settings, fixture_id=fixture_id)
    if k8s is not None:
        adapters = replace(adapters, k8s=k8s)
    run = RunInfo(run_id=new_id(), dedup_key="test", attempt=1)
    context = TriageContext(run=run, settings=settings, adapters=adapters, fixture_id=fixture_id)
    return Runtime(context=context)


def _message(fixture_id: str, **update: Any) -> IncidentMessage:
    """The fixture's incident message, with the fields in ``update`` changed."""
    data = load_fixture(fixture_id).incident | update
    return IncidentMessage.model_validate(data)


def _container(
    name: str,
    *,
    restarts: int = 0,
    exit_code: int | None = None,
    waiting: str | None = None,
    running: bool = False,
) -> ContainerStatus:
    return ContainerStatus(
        name=name,
        image=f"registry.example/{name}:1",
        image_id=None,
        restart_count=restarts,
        state="running" if running else "waiting",
        waiting_reason=None if running else waiting,
        last_terminated=None
        if exit_code is None
        else Terminated(reason="Error", exit_code=exit_code, signal=None),
        ready=running,
    )


def _pod(name: str, *containers: ContainerStatus, owner: WorkloadRef | None = ORDERS) -> PodInfo:
    return PodInfo(
        name=name,
        namespace="shop",
        node="node-a",
        phase="Running",
        containers=list(containers),
        annotations={},
        owner=owner,
    )


def _note_ids(update: Mapping[str, Any]) -> list[str]:
    return [note.id for note in update["notes"]]


async def test_resolve_pod_to_owner_workload():
    update = await resolve_target({"message": _message("python-keyerror")}, _runtime())

    assert update["target"] == Target(
        cluster="local",
        namespace="shop",
        workload=ORDERS,
        pod=ORDERS_POD,
        container="orders",
        node=ORDERS_NODE,
        images=[
            ImageRef(
                container="orders",
                image="123456789012.dkr.ecr.eu-west-1.amazonaws.com/shop/orders:2.31.0",
                digest="sha256:5f2f05f4f0055b7b01eaf88dc657d8c6e80d0884e2ea020a06d14cb1ef8314d4",
            ),
            ImageRef(
                container="istio-proxy",
                image="docker.io/istio/proxyv2:1.27.3",
                digest="sha256:6d4cbd3407cfb454a06887ca074e15ef0ead05e641f98496d4f2756b57a92527",
            ),
        ],
    )
    assert update["notes"] == []


async def test_resolve_workload_to_failing_pod():
    # checkout-web has a healthy pod of the old ReplicaSet and a pod that cannot pull
    # its image: the failing pod is the target.
    update = await resolve_target(
        {"message": _message("infra-imagepull")}, _runtime("infra-imagepull")
    )
    target = update["target"]
    assert target.workload == WorkloadRef(kind="Deployment", name="checkout-web")
    assert (target.pod, target.container) == ("checkout-web-7b9d4c6f5-k8m2p", "checkout-web")
    assert target.node == ORDERS_NODE
    assert update["notes"] == []

    # Without a crash or a waiting reason anywhere, the pod with the most restarts wins.
    k8s = FakeK8s(
        pods=[
            _pod("orders-6c8b9d7f5-bcdfg", _container("orders", restarts=1, running=True)),
            _pod("orders-6c8b9d7f5-hjklm", _container("orders", restarts=4, running=True)),
        ]
    )
    message = _message(
        "python-keyerror", pod=None, workload={"kind": "Deployment", "name": "orders"}
    )
    update = await resolve_target({"message": message}, _runtime(k8s=k8s))
    assert update["target"].pod == "orders-6c8b9d7f5-hjklm"


async def test_resolve_pod_gone_falls_back_to_workload():
    # The pod in the message was replaced after a crash. Its name follows the
    # Deployment pattern <name>-<pod-template-hash>-<suffix>, so the step looks for the
    # Deployment's current failing pod.
    gone = "orders-7d9f8b6c5d-zz9qp"
    update = await resolve_target({"message": _message("python-keyerror", pod=gone)}, _runtime())

    target = update["target"]
    assert (target.workload, target.pod, target.container) == (ORDERS, ORDERS_POD, "orders")
    assert target.node == ORDERS_NODE
    assert _note_ids(update) == ["note:target:pod_replaced"]
    assert gone in update["notes"][0].text
    assert ORDERS_POD in update["notes"][0].text

    # With a workload in the message, that workload is used.
    replaced = _pod(
        "orders-5b7c9d8f6-q2w4z", _container("orders", restarts=2, exit_code=1, waiting="Error")
    )
    k8s = FakeK8s(pods=[replaced])
    message = _message(
        "python-keyerror", pod=gone, workload={"kind": "Deployment", "name": "orders"}
    )
    update = await resolve_target({"message": message}, _runtime(k8s=k8s))
    assert (update["target"].workload, update["target"].pod) == (ORDERS, replaced.name)
    assert _note_ids(update) == ["note:target:pod_replaced"]


@pytest.mark.parametrize(
    "pod",
    [
        # A Deployment-style name whose Deployment has no pods.
        "orders-7d9f8b6c5d-zz9qp",
        # A name that follows no Deployment pattern, such as a deleted Job pod.
        "billing-invoices-29870100-7xk2p",
    ],
)
async def test_resolve_nothing_found_continues(pod):
    runtime = _runtime(k8s=FakeK8s())
    message = _message("python-keyerror", pod=pod, container="orders")

    update = await resolve_target({"message": message}, runtime)

    # The target holds what the message says, and a note says that nothing was found.
    assert update["target"] == Target(
        cluster="local",
        namespace="shop",
        workload=None,
        pod=pod,
        container="orders",
        node=None,
        images=[],
    )
    assert _note_ids(update) == ["note:target:not_found"]
    assert pod in update["notes"][0].text

    # The run goes on: collect_k8s succeeds with what there is.
    collected = await collect_k8s({"message": message, **update}, runtime)
    assert collected["k8s_evidence"] == []
    assert collected["log_text"] == ""


@pytest.mark.parametrize(
    ("pod", "deployment"),
    [
        ("orders-7d9f8b6c5d-zz9qp", "orders"),
        # A Deployment name is a DNS subdomain: it may hold dots.
        ("orders.v2-7d9f8b6c5d-zz9qp", "orders.v2"),
        ("api.payments-5b7c9d8f6-q2w4z", "api.payments"),
        # No Deployment pattern: a Job pod, or a name that starts or ends badly.
        ("billing-invoices-29870100-7xk2p", None),
        (".orders-7d9f8b6c5d-zz9qp", None),
        ("orders.-7d9f8b6c5d-zz9qp", None),
    ],
)
def test_deployment_of_a_pod_name(pod, deployment):
    expected = None if deployment is None else WorkloadRef(kind="Deployment", name=deployment)
    assert _deployment_of(pod) == expected


async def test_resolve_workload_without_pods_keeps_the_workload():
    message = _message(
        "python-keyerror", pod=None, workload={"kind": "Deployment", "name": "orders"}
    )
    update = await resolve_target({"message": message}, _runtime(k8s=FakeK8s()))

    assert update["target"].workload == ORDERS
    assert update["target"].pod is None
    assert _note_ids(update) == ["note:target:not_found"]


async def test_pick_container_ignores_healthy_sidecar():
    # In the fixture, the healthy istio-proxy sidecar runs next to the crashing app.
    update = await resolve_target({"message": _message("python-keyerror")}, _runtime())
    assert update["target"].container == "orders"

    # A sidecar is never picked by default, even with more restarts and a failed run:
    # the app container wins, though it only waits to start.
    sidecar = _container("istio-proxy", restarts=7, exit_code=137, running=True)
    app = _container("orders", waiting="CreateContainerConfigError")
    agent = _container("datadog-agent", restarts=3, running=True)
    k8s = FakeK8s(pods=[_pod(ORDERS_POD, sidecar, app, agent)])
    update = await resolve_target({"message": _message("python-keyerror")}, _runtime(k8s=k8s))
    assert update["target"].container == "orders"

    # Among the other containers, a non-zero last termination wins over restarts.
    crashed = _container("worker", restarts=1, exit_code=1, waiting="CrashLoopBackOff")
    busy = _container("orders", restarts=5, exit_code=0, running=True)
    k8s = FakeK8s(pods=[_pod(ORDERS_POD, sidecar, busy, crashed)])
    update = await resolve_target({"message": _message("python-keyerror")}, _runtime(k8s=k8s))
    assert update["target"].container == "worker"

    # The container named in the message wins, even a sidecar.
    message = _message("python-keyerror", container="istio-proxy")
    update = await resolve_target({"message": message}, _runtime())
    assert update["target"].container == "istio-proxy"

    # A pod whose only container has a sidecar's name, such as an Envoy gateway.
    k8s = FakeK8s(pods=[_pod(ORDERS_POD, _container("envoy", restarts=2, running=True))])
    update = await resolve_target({"message": _message("python-keyerror")}, _runtime(k8s=k8s))
    assert update["target"].container == "envoy"


async def test_resolve_unknown_container_is_a_note():
    message = _message("python-keyerror", container="ordrs")
    update = await resolve_target({"message": message}, _runtime())

    assert update["target"].container == "orders"
    assert _note_ids(update) == ["note:target:container_not_found"]
    assert "ordrs" in update["notes"][0].text


async def test_resolve_checks_the_cluster():
    with pytest.raises(ValueError, match="cluster 'prod-eu'"):
        await resolve_target(
            {"message": _message("python-keyerror", cluster="prod-eu")}, _runtime()
        )

    update = await resolve_target(
        {"message": _message("python-keyerror", cluster="prod-eu")},
        _runtime(cluster_name="prod-eu"),
    )
    assert update["target"].cluster == "prod-eu"
