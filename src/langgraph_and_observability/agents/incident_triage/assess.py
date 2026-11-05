"""The ``assess`` step: whether to collect code evidence (spec §9.2, §9.5).

The rules are deterministic and live in this module; their tests are table-driven
(spec §11 P20). They read the Kubernetes evidence of ``collect_k8s`` and the stack
frames parsed from the logs.

Infrastructure or configuration signals. Each one's name is the Kubernetes reason or
the node's problem:

- a container status (``k8s:status:<container>``) whose ``reason`` (of the last
  termination) or ``waiting_reason`` is in ``INFRA_REASONS``. ``Error``,
  ``Completed`` and ``CrashLoopBackOff`` are not signals by themselves;
- an event (``k8s:event:<n>``) whose ``reason`` is in ``INFRA_REASONS``: an event of
  type ``Warning``, or of any type for the reasons in ``EVENT_ANY_TYPE_REASONS``;
- a node (``k8s:node:<node>``) whose condition in meta (the first unhealthy one) is in
  ``NODE_SIGNALS`` and unhealthy: ``Ready`` not ``True``, or another condition ``True``.

Application signals:

- ``stack_trace``: the frames hold at least one app frame;
- ``exit_code:<n>``: a container's last run ended with the non-zero exit code ``n``,
  when there is no infrastructure signal.

Each signal is listed once, in the order first seen. The rule: collect code evidence
unless there are infrastructure signals and no application signal. ``reasons`` has one
sentence per decision: each signal (with the IDs of the items it came from), the logs,
each exit code, and the decision itself. A reason names signals and evidence IDs, never
evidence text.
"""

from collections.abc import Sequence
from typing import Literal

from langgraph.runtime import Runtime

from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace import parse_frames
from langgraph_and_observability.agents.incident_triage.context import TriageContext
from langgraph_and_observability.agents.incident_triage.schemas import (
    Assessment,
    EvidenceItem,
    Frame,
)
from langgraph_and_observability.agents.incident_triage.state import TriageState

# The Kubernetes reasons that are infrastructure or configuration signals (spec §9.5).
# Each reason is its own signal's name.
INFRA_REASONS: tuple[str, ...] = (
    "OOMKilled",
    "ImagePullBackOff",
    "ErrImagePull",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CreateContainerError",
    "FailedScheduling",
    "FailedMount",
    "FailedAttachVolume",
    "Evicted",
    "Unhealthy",
)

# The event reasons that are signals whatever the event's type. Other reasons count
# only on Warning events.
EVENT_ANY_TYPE_REASONS: frozenset[str] = frozenset({"Evicted"})

# The node conditions that are signals when unhealthy, with the signal's name.
NODE_SIGNALS: dict[str, str] = {
    "Ready": "NodeNotReady",
    "MemoryPressure": "MemoryPressure",
    "DiskPressure": "DiskPressure",
    "PIDPressure": "PIDPressure",
    "NetworkUnavailable": "NetworkUnavailable",
}

# The application signal of a stack trace with at least one app frame.
STACK_TRACE = "stack_trace"


def assess(k8s: Sequence[EvidenceItem], frames: Sequence[Frame], *, has_logs: bool) -> Assessment:
    """Decide whether to collect code evidence, from the evidence and the parsed frames.

    ``has_logs`` says whether there was log text to parse. It changes only the reasons.
    """
    infra = _infra_signals(k8s)
    app: list[str] = []
    reasons = [
        f"Infrastructure signal {signal}, from {_ids(ids)}." for signal, ids in infra.items()
    ]

    app_frames = sum(frame.is_app_frame for frame in frames)
    if app_frames:
        app.append(STACK_TRACE)
        plural = "" if app_frames == 1 else "s"
        reasons.append(
            f"Application signal {STACK_TRACE}: the logs hold {app_frames} app frame{plural}."
        )
    elif has_logs:
        reasons.append("No stack trace with app frames was found in the logs.")
    else:
        reasons.append("No logs were available, so no stack trace could be parsed.")

    for code, ids in _exit_codes(k8s).items():
        if infra:
            reasons.append(
                f"Exit code {code} ({_ids(ids)}) is not an application signal, "
                "because there are infrastructure signals."
            )
        else:
            signal = f"exit_code:{code}"
            app.append(signal)
            reasons.append(f"Application signal {signal}, from {_ids(ids)}.")

    collect_code = bool(app) or not infra
    reasons.append(_decision(collect_code, list(infra), app))
    return Assessment(
        collect_code=collect_code,
        infra_signals=list(infra),
        app_signals=app,
        reasons=reasons,
    )


# The graph's nodes take (state, runtime). This one reads only the state.
async def assess_node(state: TriageState, runtime: Runtime[TriageContext]) -> TriageState:  # pylint: disable=unused-argument
    """The graph's ``assess`` step. Returns ``{"assessment": Assessment}``.

    It parses the frames of ``log_text`` and assesses them with ``k8s_evidence``. It
    reads only the state.
    """
    log_text = state.get("log_text", "")
    assessment = assess(
        state.get("k8s_evidence", []),
        parse_frames(log_text),
        has_logs=bool(log_text.strip()),
    )
    return {"assessment": assessment}


def route_after_assess(state: TriageState) -> Literal["collect_code", "analyze"]:
    """The step after ``assess``: ``collect_code`` if the assessment says so, else ``analyze``."""
    return "collect_code" if state["assessment"].collect_code else "analyze"


def _infra_signals(k8s: Sequence[EvidenceItem]) -> dict[str, list[str]]:
    """Each infrastructure signal, in the order first seen, with the IDs of its items."""
    found: dict[str, list[str]] = {}
    for item in k8s:
        for signal in _item_signals(item):
            ids = found.setdefault(signal, [])
            if item.id not in ids:
                ids.append(item.id)
    return found


def _item_signals(item: EvidenceItem) -> list[str]:
    """The infrastructure signals of one evidence item."""
    meta = item.meta
    match item.kind:
        case "k8s_status":
            reasons = (meta.get("reason", ""), meta.get("waiting_reason", ""))
            return [reason for reason in reasons if reason in INFRA_REASONS]
        case "k8s_event":
            reason = meta.get("reason", "")
            counts = meta.get("type") == "Warning" or reason in EVENT_ANY_TYPE_REASONS
            return [reason] if reason in INFRA_REASONS and counts else []
        case "node":
            condition, status = meta.get("type", ""), meta.get("status", "")
            signal = NODE_SIGNALS.get(condition)
            unhealthy = status != "True" if condition == "Ready" else status == "True"
            return [signal] if signal is not None and unhealthy else []
        case _:
            return []


def _exit_codes(k8s: Sequence[EvidenceItem]) -> dict[int, list[str]]:
    """Each non-zero exit code of a container's last run, with the IDs of its statuses."""
    found: dict[int, list[str]] = {}
    for item in k8s:
        if item.kind != "k8s_status":
            continue
        try:
            code = int(item.meta.get("exit_code", ""))
        except ValueError:
            continue
        if code != 0:
            found.setdefault(code, []).append(item.id)
    return found


def _ids(ids: list[str]) -> str:
    return ", ".join(ids)


def _decision(collect_code: bool, infra: list[str], app: list[str]) -> str:
    """The reason for the decision, naming the signals it rests on."""
    if not collect_code:
        return (
            f"Skip code evidence: there are infrastructure signals ({', '.join(infra)}) "
            "and no application signal."
        )
    if not app:
        return "Collect code evidence: there is no infrastructure signal."
    reason = f"Collect code evidence: there are application signals ({', '.join(app)})"
    if infra:
        reason += f" as well as infrastructure signals ({', '.join(infra)})"
    return reason + "."
