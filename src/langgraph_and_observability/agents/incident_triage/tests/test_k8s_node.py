"""The collect_k8s step and the evidence helpers (spec §8.5, §9.2, §9.3, §9.10).

The tests resolve the target first, as the graph does, then collect the evidence. They
use the agent's incident fixtures through the fake adapter, and small in-test pods for
the edge cases (Review Focus 3).
"""

import copy
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, get_args

from langgraph.runtime import Runtime

from langgraph_and_observability.agent_spec import RunInfo
from langgraph_and_observability.agents.incident_triage.adapters import build_adapters
from langgraph_and_observability.agents.incident_triage.adapters.k8s.fake import FakeK8s
from langgraph_and_observability.agents.incident_triage.adapters.k8s.interface import (
    ContainerStatus,
    K8sEvent,
    NodeCondition,
    PodInfo,
    Terminated,
)
from langgraph_and_observability.agents.incident_triage.context import TriageContext
from langgraph_and_observability.agents.incident_triage.evidence import (
    TRUNCATED,
    EvidenceKind,
    cap_text,
    decode_log,
    make_id,
    number_lines,
)
from langgraph_and_observability.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_and_observability.agents.incident_triage.nodes.k8s import MAX_EVENTS, collect_k8s
from langgraph_and_observability.agents.incident_triage.nodes.resolve import resolve_target
from langgraph_and_observability.agents.incident_triage.schemas import (
    MAX_EVIDENCE_TEXT,
    EvidenceItem,
    IncidentMessage,
    WorkloadRef,
)
from langgraph_and_observability.ids import new_id
from langgraph_and_observability.settings import load_settings

ORDERS_POD = "orders-7d9f8b6c5d-x2kqp"
ORDERS_NODE = "ip-10-0-12-34.eu-west-1.compute.internal"
APP_POD = "api-6d5b8c9f7-wq8lh"
APP_NODE = "node-a"
T0 = datetime(2026, 11, 2, 10, 0, tzinfo=UTC)


@dataclass(frozen=True)
class _UntailedK8s(FakeK8s):
    """A fake that ignores ``tail_lines``, so the step must cap the log itself."""

    async def read_logs(
        self, ns: str, pod: str, container: str, *, previous: bool, tail_lines: int
    ) -> str | None:
        return await super().read_logs(ns, pod, container, previous=previous, tail_lines=10**9)


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


def _message(fixture_id: str = "python-keyerror", **update: Any) -> IncidentMessage:
    return IncidentMessage.model_validate(load_fixture(fixture_id).incident | update)


async def _collect(
    runtime: Runtime[TriageContext], message: IncidentMessage
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run resolve_target, then collect_k8s, as the graph does. Returns both updates."""
    resolved = await resolve_target({"message": message}, runtime)
    collected = await collect_k8s({"message": message, **resolved}, runtime)
    return dict(resolved), dict(collected)


def _by_id(items: list[EvidenceItem]) -> dict[str, EvidenceItem]:
    return {item.id: item for item in items}


def _fixture_k8s(fixture_id: str = "python-keyerror", **logs: str | None) -> FakeK8s:
    """The fixture's fake, with the logs ``<container>_<current|previous>`` replaced."""
    data = copy.deepcopy(load_fixture(fixture_id).k8s)
    for key, text in logs.items():
        container, _, which = key.rpartition("_")
        data["logs"][f"{ORDERS_POD}/{container}/{which}"] = text
    return FakeK8s.from_fixture(data)


def _app_pod(*, restarts: int = 3) -> PodInfo:
    return PodInfo(
        name=APP_POD,
        namespace="shop",
        node=APP_NODE,
        phase="Running",
        containers=[
            ContainerStatus(
                name="api",
                image="registry.example/api:1",
                image_id=None,
                restart_count=restarts,
                state="waiting",
                waiting_reason="CrashLoopBackOff",
                last_terminated=Terminated(reason="Error", exit_code=2, signal=None),
                ready=False,
            )
        ],
        annotations={},
        owner=WorkloadRef(kind="Deployment", name="api"),
    )


def _event(reason: str, last_seen: datetime | None, message: str = "m") -> K8sEvent:
    return K8sEvent(
        reason=reason,
        message=message,
        type="Warning",
        count=1,
        last_seen=last_seen,
        involved_kind="Pod",
        involved_name=APP_POD,
    )


def _app_message() -> IncidentMessage:
    return _message(pod=APP_POD, container=None)


async def test_collect_k8s_ids_unique_and_tail_capped():
    _, collected = await _collect(_runtime(), _message())
    evidence = collected["k8s_evidence"]
    ids = [item.id for item in evidence]

    assert len(ids) == len(set(ids))
    assert ids == [
        "k8s:status:orders",
        "k8s:status:istio-proxy",
        *(f"k8s:event:{n}" for n in range(1, 6)),
        f"k8s:node:{ORDERS_NODE}",
        "log:previous:orders",
    ]
    items = _by_id(evidence)
    assert items["k8s:status:orders"].kind == "k8s_status"
    assert items["k8s:status:orders"].meta == {
        "state": "waiting",
        "reason": "Error",
        "waiting_reason": "CrashLoopBackOff",
        "exit_code": "1",
        "signal": "",
        "restart_count": "6",
    }
    assert items["k8s:status:istio-proxy"].meta == {
        "state": "running",
        "reason": "",
        "waiting_reason": "",
        "exit_code": "",
        "signal": "",
        "restart_count": "0",
    }
    # Events are ordered by last_seen, then reason: the Deployment's older event first.
    assert [items[f"k8s:event:{n}"].meta for n in range(1, 6)] == [
        {"reason": "ScalingReplicaSet", "type": "Normal"},
        {"reason": "BackOff", "type": "Warning"},
        {"reason": "Created", "type": "Normal"},
        {"reason": "Pulled", "type": "Normal"},
        {"reason": "Started", "type": "Normal"},
    ]
    assert "Back-off restarting failed container orders" in items["k8s:event:2"].text
    assert items[f"k8s:node:{ORDERS_NODE}"].kind == "node"
    assert items[f"k8s:node:{ORDERS_NODE}"].meta == {"type": "Ready", "status": "True"}
    assert "MemoryPressure" in items[f"k8s:node:{ORDERS_NODE}"].text

    # The previous log is the line-numbered tail; log_text is the same lines, unnumbered.
    log_lines = items["log:previous:orders"].text.split("\n")
    assert log_lines[0] == (
        "1: 2026-11-02 10:01:12,481 INFO orders.worker polling "
        "https://sqs.eu-west-1.amazonaws.com/123456789012/orders-paid"
    )
    assert log_lines[-1] == f"{len(log_lines)}: KeyError: 'customer'"
    assert collected["log_text"].split("\n")[-1] == "KeyError: 'customer'"
    assert len(collected["log_text"].split("\n")) == len(log_lines)
    assert collected["notes"] == []


async def test_collect_k8s_caps_a_long_log():
    # 1,000 lines of 100 characters, from an adapter that ignores tail_lines.
    lines = [f"line {n:04} " + "x" * 90 for n in range(1, 1001)]
    k8s = _UntailedK8s(pods=[_app_pod()], logs={f"{APP_POD}/api/previous": "\n".join(lines)})

    _, collected = await _collect(_runtime(k8s=k8s, log_tail_lines=50), _app_message())

    # log_text holds at most log_tail_lines lines: the end of the log.
    assert collected["log_text"].split("\n") == lines[-50:]
    # The evidence text keeps the end too, in whole lines, within the size cap.
    text = _by_id(collected["k8s_evidence"])["log:previous:api"].text
    assert len(text) <= MAX_EVIDENCE_TEXT
    first, *kept = text.split("\n")
    assert first == TRUNCATED
    assert kept[-1] == f"50: {lines[-1]}"
    assert all(re.fullmatch(r"\d+: line \d{4} x{90}", line) for line in kept)


async def test_previous_logs_unavailable_is_a_note():
    # A first crash, log rotation or an API 400: the adapter has no previous log.
    k8s = _fixture_k8s(orders_previous=None, orders_current="starting\nready\n")
    _, collected = await _collect(_runtime(k8s=k8s), _message())

    notes = collected["notes"]
    assert [note.id for note in notes] == ["note:logs:previous_unavailable"]
    assert notes[0].kind == "note"
    assert "orders" in notes[0].text
    ids = [item.id for item in collected["k8s_evidence"]]
    assert "log:current:orders" in ids
    assert "log:previous:orders" not in ids
    assert _by_id(collected["k8s_evidence"])["log:current:orders"].text == "1: starting\n2: ready"
    # Without a previous log, log_text is the current log.
    assert collected["log_text"] == "starting\nready"

    # A container that never started has neither log: the step still succeeds.
    _, collected = await _collect(_runtime("infra-imagepull"), _message("infra-imagepull"))
    assert [note.id for note in collected["notes"]] == ["note:logs:previous_unavailable"]
    assert not any(item.kind == "log" for item in collected["k8s_evidence"])
    assert collected["log_text"] == ""


def test_decode_log_bytes_and_long_lines():
    assert "boom" in decode_log(b"\xff\xfeboom")
    assert decode_log(b"caf\xc3\xa9\n") == "café\n"

    assert decode_log("x" * 10_000) == "x" * 2000 + "…[truncated]"
    assert decode_log("short\n" + "y" * 3000 + "\nend") == (
        "short\n" + "y" * 2000 + "…[truncated]" + "\nend"
    )
    assert decode_log("abcdef", max_line=3) == "abc…[truncated]"
    assert decode_log("abc", max_line=3) == "abc"


async def test_evidence_is_redacted():
    pod = _app_pod()
    k8s = FakeK8s(
        pods=[pod],
        logs={
            f"{APP_POD}/api/previous": "connecting to db\npassword=x\npanic: auth failed\n",
            f"{APP_POD}/api/current": "token=abc123secret\n",
        },
        events=[_event("Failed", T0, "pull failed: Authorization: Bearer abcdefghijklmnopqrstuv")],
    )
    _, collected = await _collect(_runtime(k8s=k8s), _app_message())
    items = _by_id(collected["k8s_evidence"])

    assert (
        items["log:previous:api"].text
        == "1: connecting to db\n2: password=[REDACTED]\n3: panic: auth failed"
    )
    assert items["log:current:api"].text == "1: token=[REDACTED]"
    assert "abcdefghijklmnopqrstuv" not in items["k8s:event:1"].text
    assert "[REDACTED]" in items["k8s:event:1"].text
    assert collected["log_text"] == "connecting to db\npassword=[REDACTED]\npanic: auth failed"
    for item in collected["k8s_evidence"]:
        assert "password=x" not in item.text
        assert "abc123secret" not in item.text


async def test_events_are_ordered_and_capped():
    # More events than the cap, given out of order: the newest MAX_EVENTS are kept,
    # numbered by last_seen, then reason, so the IDs are the same on every run.
    events = [_event(f"Reason{n:02}", T0 + timedelta(minutes=n)) for n in range(MAX_EVENTS + 10)]
    tie = [_event("Unhealthy", T0 + timedelta(hours=5)), _event("BackOff", T0 + timedelta(hours=5))]
    pod = _app_pod()

    runs = []
    for order in (events + tie, list(reversed(events + tie))):
        k8s = FakeK8s(pods=[pod], events=order)
        _, collected = await _collect(_runtime(k8s=k8s), _app_message())
        runs.append(
            [
                (item.id, item.meta["reason"])
                for item in collected["k8s_evidence"]
                if item.kind == "k8s_event"
            ]
        )

    assert runs[0] == runs[1]
    assert len(runs[0]) == MAX_EVENTS
    assert [event_id for event_id, _ in runs[0]] == [
        f"k8s:event:{n}" for n in range(1, MAX_EVENTS + 1)
    ]
    assert runs[0][0] == ("k8s:event:1", "Reason12")
    assert runs[0][-2:] == [
        (f"k8s:event:{MAX_EVENTS - 1}", "BackOff"),
        (f"k8s:event:{MAX_EVENTS}", "Unhealthy"),
    ]


async def test_event_without_time_and_workload_events():
    # An event without last_seen sorts first; the owner workload's events are collected.
    deployment_event = replace(
        _event("ScalingReplicaSet", T0), involved_kind="Deployment", involved_name="api"
    )
    k8s = FakeK8s(
        pods=[_app_pod()], events=[_event("BackOff", T0), _event("Failed", None), deployment_event]
    )
    _, collected = await _collect(_runtime(k8s=k8s), _app_message())

    reasons = [
        item.meta["reason"] for item in collected["k8s_evidence"] if item.kind == "k8s_event"
    ]
    assert reasons == ["Failed", "BackOff", "ScalingReplicaSet"]


async def test_node_item_names_the_unhealthy_condition():
    def conditions(*pairs: tuple[str, str]) -> list[NodeCondition]:
        return [NodeCondition(type=kind, status=status, reason=None) for kind, status in pairs]

    cases = [
        (
            conditions(("Ready", "True"), ("MemoryPressure", "True")),
            {"type": "MemoryPressure", "status": "True"},
        ),
        (
            conditions(("MemoryPressure", "False"), ("Ready", "Unknown")),
            {"type": "Ready", "status": "Unknown"},
        ),
        (
            conditions(("DiskPressure", "False"), ("Ready", "True")),
            {"type": "Ready", "status": "True"},
        ),
        ([], {"type": "", "status": ""}),
    ]
    for node_conditions, meta in cases:
        k8s = FakeK8s(pods=[_app_pod()], nodes={APP_NODE: node_conditions})
        _, collected = await _collect(_runtime(k8s=k8s), _app_message())
        assert _by_id(collected["k8s_evidence"])[f"k8s:node:{APP_NODE}"].meta == meta


def test_cap_text_keeps_start_or_end():
    assert cap_text("short") == "short"
    assert cap_text("a" * MAX_EVIDENCE_TEXT) == "a" * MAX_EVIDENCE_TEXT

    # Other text keeps its start.
    capped = cap_text("a" * 5000)
    assert len(capped) == MAX_EVIDENCE_TEXT
    assert capped == "a" * (MAX_EVIDENCE_TEXT - len(TRUNCATED)) + TRUNCATED

    # A log keeps its end, from a line start.
    log = number_lines("\n".join(f"line {n}" for n in range(1, 101)))
    capped = cap_text(log, 120, keep="end")
    assert len(capped) <= 120
    assert capped.startswith(TRUNCATED + "\n")
    assert capped.endswith("100: line 100")
    assert re.fullmatch(r"\d+: line \d+", capped.split("\n")[1])

    # A single line longer than the cap keeps its last characters.
    assert cap_text("b" * 50 + "END", 20, keep="end") == TRUNCATED + "\n" + "b" * 4 + "END"


def test_number_lines_and_make_id():
    assert number_lines("a\nb\n") == "1: a\n2: b"
    assert number_lines("a\nb", start=41) == "41: a\n42: b"
    assert number_lines("") == ""

    assert make_id("k8s", "event", "3") == "k8s:event:3"
    assert make_id("log", "previous", "api") == "log:previous:api"
    # A key with characters that an evidence ID does not allow is made safe.
    assert make_id("k8s", "node", "node a:1") == "k8s:node:node_a_1"
    assert make_id("note", "target", "") == "note:target:_"

    # The helper's kinds are the schema's kinds.
    assert get_args(EvidenceKind) == get_args(EvidenceItem.model_fields["kind"].annotation)
