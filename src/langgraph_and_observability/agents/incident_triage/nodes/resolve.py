"""The ``resolve_target`` step: find the pod, workload and container to triage (spec §9.3).

The message names a pod, a workload, or both, in this worker's cluster:

- A pod that exists maps to its owner workload, unless the message names the workload.
- A workload maps to its current failing pod: a pod with a container whose last run
  ended with a non-zero exit code or that waits (for example in ``CrashLoopBackOff``),
  else the pod with the most restarts.
- A pod that no longer exists (replaced after a crash, or a deleted Job pod) maps to
  the workload's current failing pod (Review Focus 1). Without a workload in the
  message, a pod name of the Deployment pattern ``<name>-<pod-template-hash>-<suffix>``
  names a candidate Deployment, which counts only if it has pods.
- When no pod is found, the target holds what the message says, the note
  ``note:target:not_found`` says so, and the run goes on.

The container is the one the message names. Otherwise it is a container whose last
run ended with a non-zero exit code, else the one with the most restarts; a sidecar
(``SIDECARS``) is never picked this way unless the pod has only sidecars (Review
Focus 2). A container that the message names but the pod lacks gives a note, and the
rule picks the container.
"""

import re

from langgraph.runtime import Runtime

from langgraph_and_observability.agents.incident_triage.adapters.k8s.interface import (
    ContainerStatus,
    PodInfo,
)
from langgraph_and_observability.agents.incident_triage.context import TriageContext, ctx
from langgraph_and_observability.agents.incident_triage.evidence import make_id, make_item
from langgraph_and_observability.agents.incident_triage.schemas import (
    EvidenceItem,
    ImageRef,
    IncidentMessage,
    Target,
    WorkloadRef,
)
from langgraph_and_observability.agents.incident_triage.state import TriageState

# Containers that run next to the app and are never the default target.
SIDECARS = frozenset(
    {"istio-proxy", "linkerd-proxy", "datadog-agent", "envoy", "vault-agent", "cloud-sql-proxy"}
)

# A Deployment's pod name: <deployment>-<pod-template-hash>-<suffix>. Kubernetes writes
# the hash (up to 10 characters) and the 5-character suffix with this alphabet
# (k8s.io/apimachinery/pkg/util/rand).
_SAFE = "bcdfghjklmnpqrstvwxz2456789"
# A Deployment name is a DNS subdomain: lower-case letters, digits, "-" and ".".
_DEPLOYMENT_POD = re.compile(
    rf"(?P<name>[a-z0-9](?:[-.a-z0-9]*[a-z0-9])?)-[{_SAFE}]{{1,10}}-[{_SAFE}]{{5}}"
)


async def resolve_target(state: TriageState, runtime: Runtime[TriageContext]) -> TriageState:
    """Resolve the message's target. Returns ``message``, ``target`` and ``notes``.

    The message is validated first: Studio sends it as JSON, and the state keeps what
    the input holds. The returned ``message`` is the ``IncidentMessage``, so the later
    steps get the model. Raises pydantic's ValidationError for an invalid message, and
    ValueError when the message names a cluster other than this worker's.
    """
    message = IncidentMessage.model_validate(state["message"])
    context = ctx(runtime, message)
    message.check_cluster(context.settings.cluster_name)
    k8s = context.adapters.k8s
    ns = message.namespace
    notes: list[EvidenceItem] = []

    workload = message.workload
    pod = None if message.pod is None else await k8s.get_pod(ns, message.pod)
    if pod is not None:
        workload = workload or pod.owner
    else:
        candidate = workload
        if candidate is None and message.pod is not None:
            candidate = _deployment_of(message.pod)
        pods = [] if candidate is None else await k8s.list_pods_for_workload(ns, candidate)
        # A Deployment guessed from the pod name counts only if it has pods.
        workload = candidate if pods or message.workload is not None else None
        pod = _failing_pod(pods)
        if pod is None:
            notes.append(_not_found(ns, message.pod, workload))
        elif message.pod is not None:
            notes.append(_replaced(message.pod, pod.name, workload))

    container = message.container
    if pod is not None:
        if container is not None and all(c.name != container for c in pod.containers):
            notes.append(_container_not_found(container, pod))
            container = None
        if container is None:
            container = _pick_container(pod.containers)

    target = Target(
        cluster=context.settings.cluster_name,
        namespace=ns,
        workload=workload,
        pod=message.pod if pod is None else pod.name,
        container=container,
        node=None if pod is None else pod.node,
        images=[]
        if pod is None
        else [
            ImageRef(container=c.name, image=c.image, digest=_digest(c.image_id))
            for c in pod.containers
        ],
    )
    return {"message": message, "target": target, "notes": notes}


def _deployment_of(pod: str) -> WorkloadRef | None:
    """The Deployment that a pod of this name would belong to, or None."""
    match = _DEPLOYMENT_POD.fullmatch(pod)
    return None if match is None else WorkloadRef(kind="Deployment", name=match["name"])


def _crashed(container: ContainerStatus) -> bool:
    """Whether the container's last run ended with a non-zero exit code."""
    return container.last_terminated is not None and container.last_terminated.exit_code != 0


def _failing_pod(pods: list[PodInfo]) -> PodInfo | None:
    """The pod with a crashed or waiting container, else the most restarts; None if none.

    Among several such pods, the one with the most restarts wins, then the first.
    """

    def rank(pod: PodInfo) -> tuple[bool, int]:
        failing = any(_crashed(c) or c.waiting_reason is not None for c in pod.containers)
        return failing, sum(c.restart_count for c in pod.containers)

    return max(pods, key=rank, default=None)


def _pick_container(containers: list[ContainerStatus]) -> str | None:
    """The default container: crashed, else the most restarts, never a sidecar by default.

    Sidecars count only in a pod that has nothing else. Ties go to the first container.
    """
    candidates = [c for c in containers if c.name not in SIDECARS] or containers
    pool = [c for c in candidates if _crashed(c)] or candidates
    chosen = max(pool, key=lambda c: c.restart_count, default=None)
    return None if chosen is None else chosen.name


def _digest(image_id: str | None) -> str | None:
    """The digest in an image ID such as ``repo@sha256:...``, or None."""
    if image_id is None or "@" not in image_id:
        return None
    return image_id.rpartition("@")[2] or None


def _describe(workload: WorkloadRef) -> str:
    return f"{workload.kind} {workload.name}"


def _not_found(ns: str, pod: str | None, workload: WorkloadRef | None) -> EvidenceItem:
    parts = []
    if pod is not None:
        parts.append(f"Pod {pod} was not found in namespace {ns}.")
    if workload is not None:
        parts.append(f"{_describe(workload)} in namespace {ns} has no pods.")
    elif pod is not None:
        parts.append("No owner workload was found for it.")
    return make_item(
        make_id("note", "target", "not_found"),
        "note",
        "Target not found",
        " ".join(parts) + " The run goes on with what the message says.",
    )


def _replaced(gone: str, pod: str, workload: WorkloadRef | None) -> EvidenceItem:
    owner = "" if workload is None else f" of {_describe(workload)}"
    return make_item(
        make_id("note", "target", "pod_replaced"),
        "note",
        "The pod in the message no longer exists",
        f"Pod {gone} no longer exists. This run triages pod {pod}{owner}, the current failing pod.",
    )


def _container_not_found(container: str, pod: PodInfo) -> EvidenceItem:
    names = ", ".join(c.name for c in pod.containers)
    return make_item(
        make_id("note", "target", "container_not_found"),
        "note",
        "The container in the message was not found",
        f"The message names container {container}, but pod {pod.name} has the containers "
        f"{names}. The run picks one of those instead.",
    )
