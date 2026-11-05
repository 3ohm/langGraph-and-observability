"""The real Kubernetes adapter, through kubernetes_asyncio (spec §9.3, §9.10).

Every call only reads (``get`` and ``list``), within the read-only RBAC of §9.3. The
adapter loads its config on its first call, never in the constructor: the in-cluster
config (``load_incluster_config``), or the kubeconfig when the process does not run in a
pod (``load_kube_config``). It loads into its own ``Configuration`` and leaves the
client's global default alone. Each call opens an ``ApiClient`` and closes it before it
returns.

Errors: a missing object (404) is None, ``[]`` or ``{}``, as the interface says. A log
that does not exist (400, such as the previous log of a container that has not
restarted, or 404) is None. Every other ``ApiException`` goes up: the graph's retry
policy retries a 429 or a 5xx (the example's ``policies.is_transient``) and fails on
the rest.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal, cast

import aiohttp
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from langgraph_and_observability.agents.incident_triage.adapters.k8s.interface import (
    ContainerStatus,
    K8sAdapter,
    K8sEvent,
    NodeCondition,
    PodInfo,
    Terminated,
)
from langgraph_and_observability.agents.incident_triage.schemas import WorkloadRef

type _Workload = client.V1Deployment | client.V1StatefulSet | client.V1DaemonSet | client.V1Job

# Owners that are workloads themselves. A ReplicaSet leads to its Deployment.
_WORKLOAD_OWNERS: dict[str, Literal["StatefulSet", "DaemonSet", "Job"]] = {
    "StatefulSet": "StatefulSet",
    "DaemonSet": "DaemonSet",
    "Job": "Job",
}

# Log answers that mean "there is no such log".
_NO_LOG = (400, 404)


class RealK8s(K8sAdapter):
    """Reads pods, workloads, events, logs and nodes from the cluster that it runs in."""

    def __init__(self) -> None:
        self._configuration: client.Configuration | None = None
        self._lock = asyncio.Lock()

    async def get_pod(self, ns: str, name: str) -> PodInfo | None:
        async with self._api() as api:
            pod = await _unless_missing(client.CoreV1Api(api).read_namespaced_pod(name, ns))
            if pod is None:
                return None
            return _pod_info(pod, await _owner(api, ns, pod, {}))

    async def list_pods_for_workload(self, ns: str, wl: WorkloadRef) -> list[PodInfo]:
        """The pods that the workload's selector matches and the workload owns."""
        async with self._api() as api:
            workload = await _read_workload(api, ns, wl)
            if workload is None:
                return []
            pods = await client.CoreV1Api(api).list_namespaced_pod(
                ns, label_selector=_label_selector(workload.spec.selector)
            )
            # The pods of one ReplicaSet share one read of it.
            replica_sets: dict[str, WorkloadRef | None] = {}
            found = []
            for pod in pods.items:
                owner = await _owner(api, ns, pod, replica_sets)
                if owner == wl:
                    found.append(_pod_info(pod, owner))
            return found

    async def get_owner_workload(self, ns: str, pod: str) -> WorkloadRef | None:
        async with self._api() as api:
            found = await _unless_missing(client.CoreV1Api(api).read_namespaced_pod(pod, ns))
            return None if found is None else await _owner(api, ns, found, {})

    async def list_events(self, ns: str, kind: str, name: str) -> list[K8sEvent]:
        async with self._api() as api:
            events = await client.CoreV1Api(api).list_namespaced_event(
                ns, field_selector=f"involvedObject.kind={kind},involvedObject.name={name}"
            )
        return [_event(event) for event in events.items]

    async def read_logs(
        self, ns: str, pod: str, container: str, *, previous: bool, tail_lines: int
    ) -> str | None:
        """The log's tail, decoded as UTF-8 with replacement characters; None if none.

        The body is read raw (``_preload_content=False``): the client would otherwise
        fail on bytes that are not UTF-8, and parse a log that happens to be one JSON
        document. Without preloading, the client does not check the status, so this
        method does.
        """
        async with self._api() as api:
            try:
                response = cast(
                    aiohttp.ClientResponse,
                    await client.CoreV1Api(api).read_namespaced_pod_log(
                        pod,
                        ns,
                        container=container,
                        previous=previous,
                        tail_lines=tail_lines,
                        _preload_content=False,
                    ),
                )
                async with response:
                    if not 200 <= response.status <= 299:
                        raise ApiException(status=response.status, reason=response.reason)
                    raw = await response.read()
            except ApiException as exc:
                if exc.status in _NO_LOG:
                    return None
                raise
        return raw.decode("utf-8", errors="replace")

    async def get_node_conditions(self, node: str) -> list[NodeCondition]:
        async with self._api() as api:
            found = await _unless_missing(client.CoreV1Api(api).read_node(node))
        if found is None or found.status is None:
            return []
        return [
            NodeCondition(type=c.type, status=c.status, reason=c.reason)
            for c in found.status.conditions or []
        ]

    async def get_workload_annotations(self, ns: str, wl: WorkloadRef) -> dict[str, str]:
        async with self._api() as api:
            workload = await _read_workload(api, ns, wl)
        return {} if workload is None else dict(workload.metadata.annotations or {})

    @asynccontextmanager
    async def _api(self) -> AsyncIterator[client.ApiClient]:
        """An API client for one call, closed when the call ends."""
        configuration = await self._load_configuration()
        async with client.ApiClient(configuration) as api:
            yield api

    async def _load_configuration(self) -> client.Configuration:
        """The client config, loaded on the first call: in-cluster, else the kubeconfig."""
        async with self._lock:
            if self._configuration is None:
                configuration = client.Configuration()
                try:
                    # kubernetes_asyncio leaves this function without type annotations.
                    config.load_incluster_config(  # type: ignore[no-untyped-call]
                        client_configuration=configuration
                    )
                except config.ConfigException:
                    await config.load_kube_config(client_configuration=configuration)
                self._configuration = configuration
            return self._configuration


async def _unless_missing[T](call: Awaitable[T]) -> T | None:
    """The call's result, or None when the object does not exist (404)."""
    try:
        return await call
    except ApiException as exc:
        if exc.status == 404:
            return None
        raise


async def _read_workload(api: client.ApiClient, ns: str, wl: WorkloadRef) -> _Workload | None:
    """The workload object (``apps/v1`` or ``batch/v1``), or None if it does not exist."""
    call: Awaitable[_Workload]
    match wl.kind:
        case "Deployment":
            call = client.AppsV1Api(api).read_namespaced_deployment(wl.name, ns)
        case "StatefulSet":
            call = client.AppsV1Api(api).read_namespaced_stateful_set(wl.name, ns)
        case "DaemonSet":
            call = client.AppsV1Api(api).read_namespaced_daemon_set(wl.name, ns)
        case "Job":
            call = client.BatchV1Api(api).read_namespaced_job(wl.name, ns)
    return await _unless_missing(call)


def _controller(refs: list[client.V1OwnerReference] | None) -> client.V1OwnerReference | None:
    """The owner reference that is the object's controller, or None."""
    return next((ref for ref in refs or [] if ref.controller), None)


async def _owner(
    api: client.ApiClient,
    ns: str,
    pod: client.V1Pod,
    replica_sets: dict[str, WorkloadRef | None],
) -> WorkloadRef | None:
    """The workload that owns the pod. A ReplicaSet leads to the Deployment that owns it.

    ``replica_sets`` caches what each ReplicaSet leads to, by name.
    """
    ref = _controller(pod.metadata.owner_references)
    if ref is None:
        return None
    if ref.kind != "ReplicaSet":
        kind = _WORKLOAD_OWNERS.get(ref.kind)
        return None if kind is None else WorkloadRef(kind=kind, name=ref.name)
    if ref.name not in replica_sets:
        rs = await _unless_missing(client.AppsV1Api(api).read_namespaced_replica_set(ref.name, ns))
        deployment = None if rs is None else _controller(rs.metadata.owner_references)
        replica_sets[ref.name] = (
            WorkloadRef(kind="Deployment", name=deployment.name)
            if deployment is not None and deployment.kind == "Deployment"
            else None
        )
    return replica_sets[ref.name]


def _label_selector(selector: client.V1LabelSelector | None) -> str:
    """A label selector in the API's query form, such as ``app=orders,tier in (web)``."""
    if selector is None:
        return ""
    terms = [f"{key}={value}" for key, value in (selector.match_labels or {}).items()]
    for requirement in selector.match_expressions or []:
        key, values = requirement.key, ",".join(requirement.values or [])
        match requirement.operator:
            case "In":
                terms.append(f"{key} in ({values})")
            case "NotIn":
                terms.append(f"{key} notin ({values})")
            case "Exists":
                terms.append(key)
            case "DoesNotExist":
                terms.append(f"!{key}")
    return ",".join(terms)


def _pod_info(pod: client.V1Pod, owner: WorkloadRef | None) -> PodInfo:
    """The pod, with a status for each container of its spec, in the spec's order."""
    status = pod.status
    statuses = {s.name: s for s in (None if status is None else status.container_statuses) or []}
    return PodInfo(
        name=pod.metadata.name,
        namespace=pod.metadata.namespace,
        node=pod.spec.node_name or None,
        phase=(None if status is None else status.phase) or "Unknown",
        containers=[
            _container(spec.name, spec.image or "", statuses.get(spec.name))
            for spec in pod.spec.containers
        ],
        annotations=dict(pod.metadata.annotations or {}),
        owner=owner,
    )


def _container(name: str, image: str, status: client.V1ContainerStatus | None) -> ContainerStatus:
    """One container. Without a status yet (a pod that waits), it waits.

    ``last_terminated`` is how the container's last run ended: its current state when
    that has ended (a Job pod that never restarts has no ``lastState``), else its
    ``lastState``.
    """
    if status is None:
        return ContainerStatus(
            name=name,
            image=image,
            image_id=None,
            restart_count=0,
            state="waiting",
            waiting_reason=None,
            last_terminated=None,
            ready=False,
        )
    current = status.state
    waiting = None if current is None else current.waiting
    ended_now = None if current is None else current.terminated
    ended = ended_now or (None if status.last_state is None else status.last_state.terminated)
    state: Literal["waiting", "running", "terminated"] = "waiting"
    if current is not None and current.running is not None:
        state = "running"
    elif ended_now is not None:
        state = "terminated"
    return ContainerStatus(
        name=name,
        image=status.image or image,
        image_id=status.image_id or None,
        restart_count=status.restart_count or 0,
        state=state,
        waiting_reason=None if waiting is None else waiting.reason,
        last_terminated=None
        if ended is None
        else Terminated(reason=ended.reason, exit_code=ended.exit_code, signal=ended.signal),
        ready=bool(status.ready),
    )


def _event(event: client.CoreV1Event) -> K8sEvent:
    """One event. Newer events report their time and count in ``series``."""
    series = event.series
    last_seen: datetime | None = (
        event.last_timestamp
        or (None if series is None else series.last_observed_time)
        or event.event_time
        or event.first_timestamp
    )
    count = event.count or (None if series is None else series.count) or 1
    return K8sEvent(
        reason=event.reason or "",
        message=event.message or "",
        type=event.type or "",
        count=count,
        last_seen=last_seen,
        involved_kind=event.involved_object.kind or "",
        involved_name=event.involved_object.name or "",
    )
