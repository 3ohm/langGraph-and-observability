"""The assess rules and step (spec §9.2, §9.5, §11 P20).

``test_assess_table`` is the rules' table: small in-test evidence items and frames, and
the decision and signals each must give. The other tests check the signal tables, the
edge cases of each source, the step and its router, and every incident fixture, run
through resolve_target and collect_k8s as the graph runs them.
"""

import pytest
from langgraph.runtime import Runtime

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.agents.incident_triage.adapters import build_adapters
from langgraph_postgres_starter.agents.incident_triage.assess import (
    EVENT_ANY_TYPE_REASONS,
    INFRA_REASONS,
    NODE_SIGNALS,
    STACK_TRACE,
    assess,
    assess_node,
    route_after_assess,
)
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import (
    list_fixtures,
    load_fixture,
)
from langgraph_postgres_starter.agents.incident_triage.nodes.k8s import collect_k8s
from langgraph_postgres_starter.agents.incident_triage.nodes.resolve import resolve_target
from langgraph_postgres_starter.agents.incident_triage.schemas import (
    Assessment,
    EvidenceItem,
    Frame,
    IncidentMessage,
)
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.settings import load_settings

PYTHON_TRACE = """\
Traceback (most recent call last):
  File "/app/orders/worker.py", line 12, in main
    handle(message)
  File "/usr/local/lib/python3.14/site-packages/boto3/session.py", line 3, in client
    raise KeyError(name)
KeyError: 'customer'
"""


def _status(
    container: str = "app",
    *,
    reason: str = "",
    waiting_reason: str = "",
    exit_code: str = "",
    state: str = "waiting",
) -> EvidenceItem:
    """A ``k8s:status:<container>`` item with the meta that collect_k8s writes."""
    return EvidenceItem(
        id=f"k8s:status:{container}",
        kind="k8s_status",
        title=f"Status of container {container}",
        text="status",
        meta={
            "state": state,
            "reason": reason,
            "waiting_reason": waiting_reason,
            "exit_code": exit_code,
            "signal": "",
            "restart_count": "3",
        },
    )


def _event(n: int, reason: str, kind: str = "Warning") -> EvidenceItem:
    return EvidenceItem(
        id=f"k8s:event:{n}",
        kind="k8s_event",
        title=f"{kind} event {reason} on Pod/app-0",
        text="message",
        meta={"reason": reason, "type": kind},
    )


def _node(condition: str, status: str, name: str = "node-a") -> EvidenceItem:
    return EvidenceItem(
        id=f"k8s:node:{name}",
        kind="node",
        title=f"Conditions of node {name}",
        text=f"{condition}={status}",
        meta={"type": condition, "status": status},
    )


def _frame(file: str, line: int, function: str | None, *, app: bool, family: str) -> Frame:
    return Frame(file=file, line=line, function=function, is_app_frame=app, family=family)


JAVA_OOM_FRAMES = [
    _frame("java/util/Arrays.java", 3512, "java.util.Arrays.copyOf", app=False, family="jvm"),
    _frame(
        "com/example/reports/RevenueReport.java",
        88,
        "com.example.reports.RevenueReport.aggregate",
        app=True,
        family="jvm",
    ),
]
PYTHON_FRAMES = [
    _frame(
        "/usr/local/lib/python3.14/site-packages/boto3/session.py",
        3,
        "client",
        app=False,
        family="python",
    ),
    _frame("/app/orders/worker.py", 12, "main", app=True, family="python"),
]
OOMKILLED = _status(reason="OOMKilled", waiting_reason="CrashLoopBackOff", exit_code="137")


@pytest.mark.parametrize(
    ("k8s", "frames", "collect_code", "infra_signals", "app_signals"),
    [
        ([OOMKILLED], [], False, ["OOMKilled"], []),
        ([OOMKILLED], JAVA_OOM_FRAMES, True, ["OOMKilled"], ["stack_trace"]),
        (
            [_status(waiting_reason="ImagePullBackOff"), _event(1, "Failed")],
            [],
            False,
            ["ImagePullBackOff"],
            [],
        ),
        (
            [_status(waiting_reason="CreateContainerConfigError"), _event(1, "Failed")],
            [],
            False,
            ["CreateContainerConfigError"],
            [],
        ),
        (
            [
                _status(reason="Error", exit_code="137", state="running"),
                _event(1, "Unhealthy"),
                _event(2, "Unhealthy"),
                _event(3, "Killing", "Normal"),
            ],
            [],
            False,
            ["Unhealthy"],
            [],
        ),
        ([_status(state="terminated"), _event(1, "Evicted")], [], False, ["Evicted"], []),
        (
            [_status(reason="Error", exit_code="1"), _event(1, "BackOff")],
            PYTHON_FRAMES,
            True,
            [],
            ["stack_trace", "exit_code:1"],
        ),
        ([_status(reason="Error", exit_code="139")], [], True, [], ["exit_code:139"]),
        ([], [], True, [], []),
    ],
    ids=[
        "oomkilled-only",
        "oomkilled-with-java-oom-frames",
        "imagepullbackoff",
        "createcontainerconfigerror",
        "unhealthy-probes-no-trace",
        "evicted",
        "exit-1-python-trace",
        "exit-139-no-trace",
        "no-signals",
    ],
)
def test_assess_table(k8s, frames, collect_code, infra_signals, app_signals):
    assessment = assess(k8s, frames, has_logs=bool(frames))

    assert assessment.collect_code is collect_code
    assert assessment.infra_signals == infra_signals
    assert assessment.app_signals == app_signals
    # The rule (spec §9.5): skip code only with infrastructure signals and no app signal.
    assert assessment.collect_code == (bool(app_signals) or not infra_signals)
    # Every signal is named in a reason; the last reason states the decision.
    for signal in infra_signals + app_signals:
        assert any(signal in reason for reason in assessment.reasons)
    assert assessment.reasons[-1].startswith(
        "Collect code evidence" if collect_code else "Skip code evidence"
    )


def test_signal_tables():
    assert INFRA_REASONS == (
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
    assert EVENT_ANY_TYPE_REASONS == frozenset({"Evicted"})
    assert NODE_SIGNALS == {
        "Ready": "NodeNotReady",
        "MemoryPressure": "MemoryPressure",
        "DiskPressure": "DiskPressure",
        "PIDPressure": "PIDPressure",
        "NetworkUnavailable": "NetworkUnavailable",
    }
    assert STACK_TRACE == "stack_trace"


@pytest.mark.parametrize("reason", INFRA_REASONS)
def test_every_infra_reason_is_a_signal(reason):
    # From a container status (last termination or waiting) and from a Warning event.
    for item in (_status(reason=reason), _status(waiting_reason=reason), _event(1, reason)):
        assessment = assess([item], [], has_logs=True)
        assert assessment.infra_signals == [reason]
        assert assessment.collect_code is False


def test_status_error_and_completed_are_not_infra():
    for reason in ("Error", "Completed", "ContainerCannotRun"):
        assessment = assess([_status(reason=reason, exit_code="0")], [], has_logs=True)
        assert assessment.infra_signals == []
        assert assessment.app_signals == []
        assert assessment.collect_code is True
    # CrashLoopBackOff only says that the container keeps failing.
    assessment = assess([_status(waiting_reason="CrashLoopBackOff")], [], has_logs=True)
    assert assessment.infra_signals == []


def test_event_type_counts_except_for_evicted():
    normal = [_event(1, "Unhealthy", "Normal"), _event(2, "FailedScheduling", "Normal")]
    assert assess(normal, [], has_logs=True).infra_signals == []
    # Evicted counts whatever its type.
    assert assess([_event(1, "Evicted", "Normal")], [], has_logs=True).infra_signals == ["Evicted"]
    assert assess([_event(1, "FailedMount")], [], has_logs=True).infra_signals == ["FailedMount"]


@pytest.mark.parametrize(
    ("condition", "status", "signals"),
    [
        ("Ready", "False", ["NodeNotReady"]),
        ("Ready", "Unknown", ["NodeNotReady"]),
        ("Ready", "True", []),
        ("MemoryPressure", "True", ["MemoryPressure"]),
        ("DiskPressure", "True", ["DiskPressure"]),
        ("PIDPressure", "True", ["PIDPressure"]),
        ("NetworkUnavailable", "True", ["NetworkUnavailable"]),
        ("MemoryPressure", "False", []),
        ("KernelDeadlock", "True", []),
        ("", "", []),
    ],
)
def test_node_conditions(condition, status, signals):
    assert assess([_node(condition, status)], [], has_logs=True).infra_signals == signals


def test_signals_once_in_first_seen_order():
    k8s = [
        _status("app", reason="Error", exit_code="1"),
        _status("worker", waiting_reason="ImagePullBackOff"),
        _status("init", reason="OOMKilled", exit_code="137"),
        _status("sidecar", reason="OOMKilled", exit_code="137"),
        _event(1, "Unhealthy"),
        _event(2, "FailedMount"),
        _event(3, "Unhealthy"),
        _node("MemoryPressure", "True"),
    ]
    assessment = assess(k8s, PYTHON_FRAMES + PYTHON_FRAMES, has_logs=True)
    assert assessment.infra_signals == [
        "ImagePullBackOff",
        "OOMKilled",
        "Unhealthy",
        "FailedMount",
        "MemoryPressure",
    ]
    # With infrastructure signals, the exit codes are not application signals.
    assert assessment.app_signals == ["stack_trace"]
    assert assessment.collect_code is True
    # One reason per signal names it and the items it came from.
    assert "Infrastructure signal OOMKilled, from k8s:status:init, k8s:status:sidecar." in (
        assessment.reasons
    )
    assert "Infrastructure signal Unhealthy, from k8s:event:1, k8s:event:3." in assessment.reasons

    exits = [
        _status("a", reason="Error", exit_code="2"),
        _status("b", reason="Error", exit_code="2"),
        _status("c", reason="Error", exit_code="134"),
    ]
    assert assess(exits, [], has_logs=True).app_signals == ["exit_code:2", "exit_code:134"]


def test_library_frames_only_are_not_a_stack_trace():
    library = [frame for frame in PYTHON_FRAMES if not frame.is_app_frame]
    assessment = assess([OOMKILLED], library, has_logs=True)
    assert assessment.app_signals == []
    assert assessment.collect_code is False


def test_reasons_oomkilled_only():
    assessment = assess([OOMKILLED], [], has_logs=False)
    assert assessment.reasons == [
        "Infrastructure signal OOMKilled, from k8s:status:app.",
        "No logs were available, so no stack trace could be parsed.",
        "Exit code 137 (k8s:status:app) is not an application signal, "
        "because there are infrastructure signals.",
        "Skip code evidence: there are infrastructure signals (OOMKilled) "
        "and no application signal.",
    ]


def test_reasons_name_signals_without_evidence_text():
    # Evidence text, a title and a function name that must not reach the reasons.
    text = "the marker text"
    k8s = [
        _status(reason="Error", exit_code="1").model_copy(update={"text": text}),
        _event(1, "Unhealthy").model_copy(update={"text": text, "title": text}),
    ]
    frames = [_frame("/app/orders/worker.py", 12, text, app=True, family="python")]
    assessment = assess(k8s, frames, has_logs=True)

    assert assessment.reasons == [
        "Infrastructure signal Unhealthy, from k8s:event:1.",
        "Application signal stack_trace: the logs hold 1 app frame.",
        "Exit code 1 (k8s:status:app) is not an application signal, "
        "because there are infrastructure signals.",
        "Collect code evidence: there are application signals (stack_trace) "
        "as well as infrastructure signals (Unhealthy).",
    ]
    assert not any(text in reason for reason in assessment.reasons)

    no_trace = assess([_status(reason="Error", exit_code="139")], [], has_logs=True).reasons
    assert no_trace == [
        "No stack trace with app frames was found in the logs.",
        "Application signal exit_code:139, from k8s:status:app.",
        "Collect code evidence: there are application signals (exit_code:139).",
    ]
    assert assess([], [], has_logs=True).reasons == [
        "No stack trace with app frames was found in the logs.",
        "Collect code evidence: there is no infrastructure signal.",
    ]


async def test_assess_node_parses_the_log_text():
    k8s = [_status(reason="Error", exit_code="1")]
    # The step reads only the state; it needs no context.
    runtime: Runtime[TriageContext] = Runtime()

    update = await assess_node({"k8s_evidence": k8s, "log_text": PYTHON_TRACE}, runtime)
    assert set(update) == {"assessment"}
    assert update["assessment"] == assess(k8s, PYTHON_FRAMES, has_logs=True)
    assert update["assessment"].app_signals == ["stack_trace", "exit_code:1"]

    # Without log text (none collected, or blank), there are no logs to parse.
    states: list[TriageState] = [
        {"k8s_evidence": [OOMKILLED]},
        {"k8s_evidence": [OOMKILLED], "log_text": " \n"},
    ]
    for state in states:
        update = await assess_node(state, runtime)
        assert update["assessment"] == assess([OOMKILLED], [], has_logs=False)
        assert update["assessment"].collect_code is False


def test_route_after_assess():
    def state(collect_code: bool) -> TriageState:
        assessment = Assessment(
            collect_code=collect_code, infra_signals=[], app_signals=[], reasons=[]
        )
        return {"assessment": assessment}

    assert route_after_assess(state(True)) == "collect_code"
    assert route_after_assess(state(False)) == "analyze"


async def _assess_fixture(fixture_id: str) -> Assessment:
    """Run resolve_target, collect_k8s and assess_node on a fixture, as the graph does."""
    settings = load_settings(adapters="simulated")
    adapters = build_adapters(settings, fixture_id=fixture_id)
    run = RunInfo(run_id=new_id(), dedup_key="test", attempt=1)
    runtime = Runtime(
        context=TriageContext(run=run, settings=settings, adapters=adapters, fixture_id=fixture_id)
    )
    state: TriageState = {
        "message": IncidentMessage.model_validate(load_fixture(fixture_id).incident)
    }
    state.update(await resolve_target(state, runtime))
    state.update(await collect_k8s(state, runtime))
    update = await assess_node(state, runtime)
    return update["assessment"]


INFRA_FIXTURES = {
    "infra-oomkilled": ["OOMKilled"],
    "infra-imagepull": ["ImagePullBackOff"],
    "infra-probe-failure": ["Unhealthy"],
    "infra-config-error": ["CreateContainerConfigError"],
}


@pytest.mark.parametrize("fixture_id", list_fixtures())
async def test_fixtures_assess(fixture_id):
    assessment = await _assess_fixture(fixture_id)

    if fixture_id in INFRA_FIXTURES:
        assert assessment.collect_code is False
        assert assessment.infra_signals == INFRA_FIXTURES[fixture_id]
        assert assessment.app_signals == []
    elif fixture_id == "c-segfault":
        # A segfault prints no trace: the exit code alone is the application signal.
        assert assessment.collect_code is True
        assert assessment.infra_signals == []
        assert assessment.app_signals == ["exit_code:139"]
    else:
        assert assessment.collect_code is True
        assert assessment.infra_signals == []
        assert assessment.app_signals[0] == "stack_trace"


def test_fixture_set():
    # The fixture test covers the 4 infrastructure incidents and the 12 app incidents.
    fixtures = list_fixtures()
    assert len(fixtures) == 16
    assert {f for f in fixtures if f.startswith("infra-")} == set(INFRA_FIXTURES)
