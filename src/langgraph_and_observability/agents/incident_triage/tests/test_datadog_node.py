"""The collect_datadog step: its query, time window and evidence items (spec §9.2, §9.4).

The step reads its adapter from the run context. Most tests give it ``_Recording``, an
adapter that records each search and returns every event it holds, whatever the limit,
so the step must cap the list itself. One test runs it on an incident fixture through
the fake adapter, as the graph does in simulated mode.
"""

import re
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from langgraph.runtime import Runtime

from langgraph_and_observability.agent_spec import RunInfo
from langgraph_and_observability.agents.incident_triage.adapters import build_adapters
from langgraph_and_observability.agents.incident_triage.adapters.datadog.interface import (
    DatadogAdapter,
    DdEvent,
)
from langgraph_and_observability.agents.incident_triage.context import TriageContext
from langgraph_and_observability.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_and_observability.agents.incident_triage.nodes.datadog import (
    build_query,
    collect_datadog,
    incident_time,
    window,
)
from langgraph_and_observability.agents.incident_triage.schemas import (
    EVIDENCE_ID_PATTERN,
    MAX_EVIDENCE_TEXT,
    IncidentMessage,
    Target,
    WorkloadRef,
)
from langgraph_and_observability.ids import new_id
from langgraph_and_observability.redaction import REDACTED
from langgraph_and_observability.settings import load_settings

NS = "shop"
POD = "orders-7d9f8b6c5d-x2kqp"
T0 = datetime(2026, 11, 2, 10, 0, tzinfo=UTC)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass
class _Recording(DatadogAdapter):
    """Records each search and returns all its events, ignoring the limit."""

    events: list[DdEvent] = field(default_factory=list)
    calls: list[tuple[str, datetime, datetime, int]] = field(default_factory=list)

    async def search_events(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> list[DdEvent]:
        self.calls.append((query, start, end, limit))
        return list(self.events)


def _uuid7_at(when: datetime) -> UUID:
    """A UUIDv7 whose timestamp is ``when``, to the millisecond."""
    ms = (when - _EPOCH) // timedelta(milliseconds=1)
    return UUID(int=ms << 80 | 0x7 << 76 | 0x2 << 62 | 0x1234)


def _target(*, pod: str | None = POD, workload: WorkloadRef | None = None, ns: str = NS) -> Target:
    return Target(
        cluster="local",
        namespace=ns,
        workload=workload,
        pod=pod,
        container=None,
        node=None,
        images=[],
    )


def _message(fired_at: datetime | None = T0) -> IncidentMessage:
    return IncidentMessage(version=1, source="manual", namespace=NS, pod=POD, fired_at=fired_at)


def _runtime(
    datadog: DatadogAdapter, *, run_id: UUID | None = None, **overrides: Any
) -> Runtime[TriageContext]:
    """A simulated runtime whose Datadog adapter is ``datadog``."""
    settings = load_settings(**overrides)
    adapters = replace(build_adapters(settings, fixture_id="python-keyerror"), datadog=datadog)
    run = RunInfo(run_id=run_id or new_id(), dedup_key="test", attempt=1)
    context = TriageContext(
        run=run, settings=settings, adapters=adapters, fixture_id="python-keyerror"
    )
    return Runtime(context=context)


def _event(n: int, **fields: Any) -> DdEvent:
    """Event ``n``, ``n`` seconds after T0."""
    values: dict[str, Any] = {
        "id": f"evt{n}",
        "title": f"event {n}",
        "text": f"text of event {n}",
        "timestamp": T0 + timedelta(seconds=n),
        "tags": [f"kube_namespace:{NS}", f"pod_name:{POD}"],
        "source": "kubernetes",
    }
    return DdEvent(**(values | fields))


async def _collect(datadog: DatadogAdapter, **overrides: Any) -> list[Any]:
    update = await collect_datadog(
        {"message": _message(), "target": _target()}, _runtime(datadog, **overrides)
    )
    assert set(update) == {"datadog_evidence"}
    return update["datadog_evidence"]


@pytest.mark.parametrize(
    ("pod", "workload", "expected"),
    [
        (
            POD,
            WorkloadRef(kind="Deployment", name="orders"),
            f"kube_namespace:shop (pod_name:{POD} OR kube_deployment:orders)",
        ),
        (
            "batcher-0",
            WorkloadRef(kind="StatefulSet", name="batcher"),
            "kube_namespace:shop (pod_name:batcher-0 OR kube_stateful_set:batcher)",
        ),
        (POD, None, f"kube_namespace:shop pod_name:{POD}"),
        (
            None,
            WorkloadRef(kind="DaemonSet", name="node-agent"),
            "kube_namespace:shop kube_daemon_set:node-agent",
        ),
        (
            None,
            WorkloadRef(kind="Job", name="migrate-42"),
            "kube_namespace:shop kube_job:migrate-42",
        ),
        (None, None, "kube_namespace:shop"),
    ],
    ids=["pod-and-deployment", "pod-and-statefulset", "pod-only", "daemonset", "job", "neither"],
)
def test_query_for_pod_and_workload(pod, workload, expected):
    assert build_query(_target(pod=pod, workload=workload)) == expected


def test_query_escapes_special_characters():
    # Kubernetes names ([a-z0-9.-]) need no escaping: a "-" inside a value is literal.
    # Other values come from the message as given, so the query escapes what the
    # search syntax treats as special, including a leading "-" (exclusion).
    target = _target(ns="a b:c", pod="-x(1)*", workload=WorkloadRef(kind="Job", name='q"\\#'))
    assert build_query(target) == (
        r"kube_namespace:a\ b\:c (pod_name:\-x\(1\)\* OR kube_job:q\"\\\#)"
    )


def test_window_bounds():
    assert window(T0, 30) == (T0 - timedelta(minutes=30), T0 + timedelta(minutes=5))
    assert window(T0, 10) == (T0 - timedelta(minutes=10), T0 + timedelta(minutes=5))


def test_incident_time_is_fired_at_else_the_run_ids_time():
    run_id = _uuid7_at(T0 + timedelta(minutes=2, milliseconds=345))
    assert incident_time(T0, run_id) == T0
    # Without fired_at, the run's ID gives the intake time, so a resumed step queries
    # the same window as the first attempt.
    assert incident_time(None, run_id) == T0 + timedelta(minutes=2, milliseconds=345)
    assert incident_time(None, run_id) == incident_time(None, UUID(str(run_id)))

    before = datetime.now(UTC) - timedelta(milliseconds=1)
    fresh = incident_time(None, new_id())
    assert before <= fresh <= datetime.now(UTC)

    with pytest.raises(ValueError, match="UUIDv7"):
        incident_time(None, uuid4())


async def test_collect_datadog_searches_the_incident_window():
    datadog = _Recording()
    target = _target(workload=WorkloadRef(kind="Deployment", name="orders"))

    await collect_datadog({"message": _message(), "target": target}, _runtime(datadog))
    assert datadog.calls == [
        (build_query(target), T0 - timedelta(minutes=30), T0 + timedelta(minutes=5), 50)
    ]

    # Without fired_at, the window is around the time in the run's ID.
    received = T0 + timedelta(hours=1)
    runtime = _runtime(datadog, run_id=_uuid7_at(received), dd_lookback_minutes=10, dd_max_events=3)
    await collect_datadog({"message": _message(fired_at=None), "target": target}, runtime)
    assert datadog.calls[1] == (
        build_query(target),
        received - timedelta(minutes=10),
        received + timedelta(minutes=5),
        3,
    )


async def test_events_capped_and_ids():
    # Events 0 to 59, out of order (37 and 60 have no common factor).
    events = [_event(n * 37 % 60) for n in range(60)]

    items = await _collect(_Recording(events))

    # The newest dd_max_events (50), newest first.
    assert [item.id for item in items] == [f"dd:event:evt{n}" for n in range(59, 9, -1)]
    assert all(re.fullmatch(EVIDENCE_ID_PATTERN, item.id) for item in items)

    newest = items[0]
    assert newest.kind == "datadog_event"
    assert newest.title == "event 59"
    when = (T0 + timedelta(seconds=59)).isoformat()
    assert newest.text == (
        f"text of event 59\nat {when}, source kubernetes\ntags: kube_namespace:{NS}, pod_name:{POD}"
    )
    assert newest.meta == {
        "source": "kubernetes",
        "timestamp": when,
        "tags": f"kube_namespace:{NS},pod_name:{POD}",
    }

    # A smaller cap keeps fewer, still the newest.
    few = await _collect(_Recording(events), dd_max_events=3)
    assert [item.id for item in few] == ["dd:event:evt59", "dd:event:evt58", "dd:event:evt57"]


async def test_event_ids_sanitized_unique_and_stable():
    # Events 1 and 2 sanitize to the ID of event 4, the newest of the three, which
    # keeps it; the others get the first free numeric suffix. Event 3's own ID looks
    # like a suffixed one and keeps it.
    events = [
        _event(1, id="AAAA+b/c="),
        _event(2, id="AAAA=b/c+"),
        _event(3, id="AAAA_b/c__2"),
        _event(4, id="AAAA_b/c_"),
        _event(5, id=""),
        _event(6, id="ok.ID-9/x"),
    ]

    items = await _collect(_Recording(events))
    ids = {item.title: item.id for item in items}
    assert ids == {
        "event 6": "dd:event:ok.ID-9/x",
        "event 5": "dd:event:_",
        "event 4": "dd:event:AAAA_b/c_",
        "event 3": "dd:event:AAAA_b/c__2",
        "event 2": "dd:event:AAAA_b/c__3",
        "event 1": "dd:event:AAAA_b/c__4",
    }
    assert all(re.fullmatch(EVIDENCE_ID_PATTERN, item.id) for item in items)

    # The same events in another order get the same IDs.
    again = await _collect(_Recording(list(reversed(events))))
    assert {item.title: item.id for item in again} == ids


async def test_event_items_redacted_and_capped():
    events = [
        _event(
            1,
            title="Deploy failed: password=hunter2",
            text="db url postgres://app:s3cret@db:5432/orders\n" + "x" * 10_000,
            tags=["token:abc123", "kube_namespace:shop"] + [f"t{n}:v" for n in range(20)],
            source=None,
            timestamp=datetime(2026, 11, 2, 10, 0, 1),  # naive: taken as UTC
        )
    ]

    (item,) = await _collect(_Recording(events))
    assert "hunter2" not in item.title and REDACTED in item.title
    assert "s3cret" not in item.text and REDACTED in item.text
    assert len(item.text) <= MAX_EVIDENCE_TEXT
    assert "abc123" not in item.meta["tags"]
    # meta holds the first ten tags only.
    assert item.meta["tags"].count(",") == 9
    assert item.meta["source"] == ""
    assert item.meta["timestamp"] == "2026-11-02T10:00:01+00:00"

    # An event without a title or text still reads as one.
    (bare,) = await _collect(_Recording([_event(2, title="", text="", tags=[])]))
    assert bare.title == "Datadog event without a title"
    assert bare.text.startswith("(no text)\nat ")


async def test_collect_datadog_on_a_fixture():
    settings = load_settings()
    adapters = build_adapters(settings, fixture_id="python-keyerror")
    run = RunInfo(run_id=new_id(), dedup_key="test", attempt=1)
    runtime = Runtime(
        context=TriageContext(
            run=run, settings=settings, adapters=adapters, fixture_id="python-keyerror"
        )
    )
    message = IncidentMessage.model_validate(load_fixture("python-keyerror").incident)
    target = _target(workload=WorkloadRef(kind="Deployment", name="orders"))

    update = await collect_datadog({"message": message, "target": target}, runtime)

    items = update["datadog_evidence"]
    assert [item.id for item in items] == [
        "dd:event:8312004415592873007",
        "dd:event:8312004415592873006",
        "dd:event:8312004415592873008",
    ]
    assert items[0].title == "Events from the orders-7d9f8b6c5d-x2kqp Pod"
    assert items[0].meta["source"] == "kubernetes"
    assert items[0].meta["timestamp"] == "2026-11-02T10:04:31+00:00"
