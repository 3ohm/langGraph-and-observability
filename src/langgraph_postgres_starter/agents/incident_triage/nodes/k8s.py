"""The ``collect_k8s`` step: Kubernetes evidence about the target (spec §9.2, §9.3).

It collects, as evidence items (``evidence.make_item`` redacts each one and caps its
text):

- ``k8s:status:<container>`` for each container of the pod: the pod's phase and node,
  the container's state, last termination and restarts. Meta: ``state``, ``reason``
  (of the last termination), ``waiting_reason``, ``exit_code``, ``signal`` and
  ``restart_count``; a value that is not known is ``""``.
- ``k8s:event:<n>`` for the events of the pod and of its owner workload: the newest
  ``MAX_EVENTS``, numbered from 1 in the order of ``last_seen`` (an event without one
  first), then reason, kind, name and message, so a step that runs again gives the
  same IDs for the same events. Meta: ``reason``, ``type``.
- ``k8s:node:<node>``: the node's conditions. Meta: ``type`` and ``status`` of the
  first unhealthy condition (``Ready`` not ``True``, or another condition ``True``),
  else of ``Ready``.
- ``log:current:<container>`` and ``log:previous:<container>``: the target container's
  logs, each the line-numbered tail of at most ``log_tail_lines`` lines, cut at the
  start to fit (``cap_text`` with ``keep="end"``).

A previous log that does not exist (a first crash, log rotation, or an API 400) is the
note ``note:logs:previous_unavailable``, and the step succeeds (Review Focus 3).

``log_text`` is the previous log, or the current log when the previous one is missing
or empty: decoded (``decode_log``), at most ``log_tail_lines`` lines, and redacted.
"""

from datetime import UTC, datetime

from langgraph.runtime import Runtime

from langgraph_postgres_starter.agents.incident_triage.adapters.k8s.interface import (
    ContainerStatus,
    K8sAdapter,
    K8sEvent,
    NodeCondition,
    PodInfo,
)
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext, ctx
from langgraph_postgres_starter.agents.incident_triage.evidence import (
    decode_log,
    make_id,
    make_item,
    number_lines,
)
from langgraph_postgres_starter.agents.incident_triage.schemas import EvidenceItem, Target
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.redaction import redact

# The most events that the step keeps: the newest.
MAX_EVENTS = 50

# The sort time of an event without last_seen: before every other event.
_NEVER = datetime.min.replace(tzinfo=UTC)


async def collect_k8s(state: TriageState, runtime: Runtime[TriageContext]) -> TriageState:
    """Collect the target's Kubernetes evidence and log text.

    Returns ``{"k8s_evidence": [...], "log_text": str, "notes": [...]}``.
    """
    context = ctx(runtime, state.get("message"))
    k8s = context.adapters.k8s
    tail_lines = context.settings.log_tail_lines
    target = state["target"]
    evidence: list[EvidenceItem] = []
    notes: list[EvidenceItem] = []

    pod = None if target.pod is None else await k8s.get_pod(target.namespace, target.pod)
    if pod is not None:
        evidence += [_status_item(pod, container) for container in pod.containers]
    evidence += _event_items(await _events(k8s, target))
    if target.node is not None:
        evidence.append(_node_item(target.node, await k8s.get_node_conditions(target.node)))

    log_text = ""
    if pod is not None and target.container is not None:
        logs: dict[str, str | None] = {}
        for which in ("current", "previous"):
            raw = await k8s.read_logs(
                pod.namespace,
                pod.name,
                target.container,
                previous=which == "previous",
                tail_lines=tail_lines,
            )
            logs[which] = None if raw is None else redact(_tail(decode_log(raw), tail_lines))
        for which, text in logs.items():
            if text is not None:
                evidence.append(_log_item(which, pod.name, target.container, text))
        if logs["previous"] is None:
            notes.append(_previous_unavailable(pod.name, target.container))
        previous = logs["previous"]
        log_text = previous if previous and previous.strip() else logs["current"] or ""

    return {"k8s_evidence": evidence, "log_text": log_text, "notes": notes}


async def _events(k8s: K8sAdapter, target: Target) -> list[K8sEvent]:
    """The events of the pod and of its owner workload."""
    events: list[K8sEvent] = []
    if target.pod is not None:
        events += await k8s.list_events(target.namespace, "Pod", target.pod)
    if target.workload is not None:
        events += await k8s.list_events(
            target.namespace, target.workload.kind, target.workload.name
        )
    return events


def _tail(text: str, lines: int) -> str:
    """The last ``lines`` lines of ``text``, without a final newline."""
    split = text.split("\n")
    if split[-1] == "":
        split.pop()
    return "\n".join(split[max(len(split) - lines, 0) :])


def _text(value: object) -> str:
    """A meta value: ``""`` for None, else the value as text."""
    return "" if value is None else str(value)


def _status_item(pod: PodInfo, container: ContainerStatus) -> EvidenceItem:
    ended = container.last_terminated
    waiting = "" if container.waiting_reason is None else f" ({container.waiting_reason})"
    lines = [
        f"pod {pod.name}: phase {pod.phase}, node {pod.node or 'none'}",
        f"container {container.name}: state {container.state}{waiting}, "
        f"ready {str(container.ready).lower()}, "
        f"restarts {container.restart_count}",
        "last termination: none"
        if ended is None
        else f"last termination: reason {ended.reason or 'none'}, exit code {ended.exit_code}, "
        f"signal {_text(ended.signal) or 'none'}",
        f"image: {container.image}",
    ]
    if container.image_id is not None:
        lines.append(f"image ID: {container.image_id}")
    return make_item(
        make_id("k8s", "status", container.name),
        "k8s_status",
        f"Status of container {container.name} in pod {pod.name}",
        "\n".join(lines),
        {
            "state": container.state,
            "reason": "" if ended is None else _text(ended.reason),
            "waiting_reason": _text(container.waiting_reason),
            "exit_code": "" if ended is None else str(ended.exit_code),
            "signal": "" if ended is None else _text(ended.signal),
            "restart_count": str(container.restart_count),
        },
    )


def _event_key(event: K8sEvent) -> tuple[datetime, str, str, str, str]:
    seen = event.last_seen
    if seen is None:
        seen = _NEVER
    elif seen.tzinfo is None:
        seen = seen.replace(tzinfo=UTC)
    return seen, event.reason, event.involved_kind, event.involved_name, event.message


def _event_items(events: list[K8sEvent]) -> list[EvidenceItem]:
    newest = sorted(events, key=_event_key)[-MAX_EVENTS:]
    return [
        make_item(
            make_id("k8s", "event", str(number)),
            "k8s_event",
            f"{event.type} event {event.reason} on {event.involved_kind}/{event.involved_name}",
            f"{event.message}\ncount {event.count}, last seen "
            f"{'unknown' if event.last_seen is None else event.last_seen.isoformat()}",
            {"reason": event.reason, "type": event.type},
        )
        for number, event in enumerate(newest, 1)
    ]


def _unhealthy(condition: NodeCondition) -> bool:
    if condition.type == "Ready":
        return condition.status != "True"
    return condition.status == "True"


def _node_item(node: str, conditions: list[NodeCondition]) -> EvidenceItem:
    ready = [c for c in conditions if c.type == "Ready"]
    unhealthy = [c for c in ready + conditions if _unhealthy(c)]
    headline = next(iter(unhealthy or ready), None)
    text = "\n".join(
        f"{c.type}={c.status}" + ("" if c.reason is None else f" ({c.reason})") for c in conditions
    )
    return make_item(
        make_id("k8s", "node", node),
        "node",
        f"Conditions of node {node}",
        text or "The node reports no conditions; it may no longer exist.",
        {
            "type": "" if headline is None else headline.type,
            "status": "" if headline is None else headline.status,
        },
    )


def _log_item(which: str, pod: str, container: str, text: str) -> EvidenceItem:
    count = 0 if text == "" else text.count("\n") + 1
    run = "previous run" if which == "previous" else "current run"
    return make_item(
        make_id("log", which, container),
        "log",
        f"Log of the {run} of container {container} in pod {pod} (last {count} lines)",
        number_lines(text) if text else "(empty)",
        keep="end",
    )


def _previous_unavailable(pod: str, container: str) -> EvidenceItem:
    return make_item(
        make_id("note", "logs", "previous_unavailable"),
        "note",
        "No log of the previous run",
        f"Kubernetes has no log of the previous run of container {container} in pod {pod}. "
        "This happens on a first crash, after log rotation, or when the API returns 400.",
    )
