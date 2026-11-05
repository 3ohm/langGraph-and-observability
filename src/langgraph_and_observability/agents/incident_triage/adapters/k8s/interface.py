"""The Kubernetes adapter's interface (spec §9.3): read-only calls and what they return.

Every call only reads, and every call is async. ``FakeK8s`` (``fake.py``) serves an
incident fixture.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from langgraph_and_observability.agents.incident_triage.schemas import WorkloadRef


@dataclass(frozen=True)
class Terminated:
    """How a container's last run ended: its ``lastState.terminated``."""

    reason: str | None
    exit_code: int
    signal: int | None


@dataclass(frozen=True)
class ContainerStatus:
    """One container of a pod.

    ``image_id`` is the image with its digest, as the pod status reports it, or None.
    ``state`` is the container's current state; ``waiting_reason`` is set when that
    state is ``waiting`` (for example ``CrashLoopBackOff``). ``last_terminated`` is
    the end of the previous run, if there was one.
    """

    name: str
    image: str
    image_id: str | None
    restart_count: int
    state: Literal["waiting", "running", "terminated"]
    waiting_reason: str | None
    last_terminated: Terminated | None
    ready: bool


@dataclass(frozen=True)
class PodInfo:
    """A pod: its node, phase, containers and annotations, and the workload that owns it."""

    name: str
    namespace: str
    node: str | None
    phase: str
    containers: list[ContainerStatus]
    annotations: dict[str, str]
    owner: WorkloadRef | None


@dataclass(frozen=True)
class K8sEvent:
    """A Kubernetes event about the object ``involved_kind``/``involved_name``.

    ``type`` is ``Normal`` or ``Warning``. ``count`` is how many times it happened.
    """

    reason: str
    message: str
    type: str
    count: int
    last_seen: datetime | None
    involved_kind: str
    involved_name: str


@dataclass(frozen=True)
class NodeCondition:
    """One condition of a node, such as ``Ready`` or ``MemoryPressure``.

    ``status`` is ``True``, ``False`` or ``Unknown``, as Kubernetes writes it.
    """

    type: str
    status: str
    reason: str | None


class K8sAdapter(Protocol):
    """Reads pods, workloads, events, logs and nodes from the cluster."""

    async def get_pod(self, ns: str, name: str) -> PodInfo | None:
        """The pod, or None if it does not exist."""
        ...

    async def list_pods_for_workload(self, ns: str, wl: WorkloadRef) -> list[PodInfo]:
        """The pods that the workload owns."""
        ...

    async def get_owner_workload(self, ns: str, pod: str) -> WorkloadRef | None:
        """The workload that owns the pod ``pod``, or None (no such pod, or no owner)."""
        ...

    async def list_events(self, ns: str, kind: str, name: str) -> list[K8sEvent]:
        """The events about the object of kind ``kind`` (such as ``Pod``) named ``name``."""
        ...

    async def read_logs(
        self, ns: str, pod: str, container: str, *, previous: bool, tail_lines: int
    ) -> str | None:
        """The last ``tail_lines`` lines of a container's log, or None if it has none.

        With ``previous``, the log of the container's previous run.
        """
        ...

    async def get_node_conditions(self, node: str) -> list[NodeCondition]:
        """The node's conditions."""
        ...

    async def get_workload_annotations(self, ns: str, wl: WorkloadRef) -> dict[str, str]:
        """The workload's annotations."""
        ...
