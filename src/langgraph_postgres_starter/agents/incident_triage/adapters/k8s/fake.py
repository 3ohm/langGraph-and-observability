"""A Kubernetes adapter that serves an incident fixture's ``k8s.json`` (fixtures/FORMAT.md)."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Self, TypedDict

from pydantic import ConfigDict, TypeAdapter, with_config

from langgraph_postgres_starter.agents.incident_triage.adapters.call_log import NO_CALL_LOG, CallLog
from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.interface import (
    K8sAdapter,
    K8sEvent,
    NodeCondition,
    PodInfo,
)
from langgraph_postgres_starter.agents.incident_triage.schemas import WorkloadRef


# The shape of k8s.json. A missing or unknown key is an error, also in nested objects.
@with_config(ConfigDict(extra="forbid"))
class _K8sJson(TypedDict):
    pods: list[PodInfo]
    events: list[K8sEvent]
    logs: dict[str, str | None]
    nodes: dict[str, list[NodeCondition]]
    annotations: dict[str, str]


_K8S_JSON = TypeAdapter(_K8sJson)


@dataclass(frozen=True)
class FakeK8s(K8sAdapter):
    """Serves fixed pods, events, logs, node conditions and workload annotations.

    ``logs`` is keyed ``<pod>/<container>/current`` and ``<pod>/<container>/previous``.
    ``annotations`` are the annotations of the fixture's workload; every workload
    gets them. Each call appends ``k8s.<method>`` to ``calls``.
    """

    pods: list[PodInfo] = field(default_factory=list)
    events: list[K8sEvent] = field(default_factory=list)
    logs: dict[str, str | None] = field(default_factory=dict)
    nodes: dict[str, list[NodeCondition]] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    calls: CallLog = NO_CALL_LOG

    @classmethod
    def from_fixture(cls, k8s: Mapping[str, Any], calls: CallLog = NO_CALL_LOG) -> Self:
        """Build the fake from a ``k8s.json`` object. Raises pydantic's ValidationError."""
        data = _K8S_JSON.validate_python(k8s)
        return cls(
            pods=data["pods"],
            events=data["events"],
            logs=data["logs"],
            nodes=data["nodes"],
            annotations=data["annotations"],
            calls=calls,
        )

    async def get_pod(self, ns: str, name: str) -> PodInfo | None:
        self.calls.record("k8s.get_pod")
        return self._pod(ns, name)

    async def list_pods_for_workload(self, ns: str, wl: WorkloadRef) -> list[PodInfo]:
        self.calls.record("k8s.list_pods_for_workload")
        return [pod for pod in self.pods if pod.namespace == ns and pod.owner == wl]

    async def get_owner_workload(self, ns: str, pod: str) -> WorkloadRef | None:
        self.calls.record("k8s.get_owner_workload")
        found = self._pod(ns, pod)
        return None if found is None else found.owner

    async def list_events(self, ns: str, kind: str, name: str) -> list[K8sEvent]:
        # A fixture has one namespace, so ns does not filter.
        self.calls.record("k8s.list_events")
        return [e for e in self.events if (e.involved_kind, e.involved_name) == (kind, name)]

    async def read_logs(
        self, ns: str, pod: str, container: str, *, previous: bool, tail_lines: int
    ) -> str | None:
        self.calls.record("k8s.read_logs")
        if self._pod(ns, pod) is None:
            return None
        text = self.logs.get(f"{pod}/{container}/{'previous' if previous else 'current'}")
        if text is None:
            return None
        lines = text.splitlines(keepends=True)
        return "".join(lines[max(len(lines) - tail_lines, 0) :])

    async def get_node_conditions(self, node: str) -> list[NodeCondition]:
        self.calls.record("k8s.get_node_conditions")
        return list(self.nodes.get(node, []))

    async def get_workload_annotations(self, ns: str, wl: WorkloadRef) -> dict[str, str]:
        self.calls.record("k8s.get_workload_annotations")
        return dict(self.annotations)

    def _pod(self, ns: str, name: str) -> PodInfo | None:
        return next((p for p in self.pods if (p.namespace, p.name) == (ns, name)), None)
