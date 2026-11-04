"""The JVM, Node and Python stack-trace parsers (spec §9.5, §9.6 item 2).

Each app incident's crash log leads to its expected code location: the first app frame
that ``parse_frames`` returns is in the expected file (``fixture_paths.points_at``) and
in the expected lines. Small in-test traces cover each family's library rules and the
format variants of the runtimes.
"""

import pytest

from langgraph_postgres_starter.agents.incident_triage.codeintel.stacktrace import (
    Family,
    detect_families,
    jvm,
    node,
    parse_frames,
    python,
)
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import Fixture, load_fixture
from langgraph_postgres_starter.agents.incident_triage.schemas import Frame
from langgraph_postgres_starter.agents.incident_triage.tests.fixture_paths import points_at

# The app incidents of these families, with the family of each crash log.
FIXTURE_FAMILIES = {
    "js-typeerror": Family.NODE,
    "ts-undefined-property": Family.NODE,
    "python-keyerror": Family.PYTHON,
    "java-spring-npe": Family.JVM,
    "java-plain-arithmetic": Family.JVM,
    "kotlin-illegal-state": Family.JVM,
    "scala-match-error": Family.JVM,
}


def crash_log(fixture: Fixture) -> str:
    """The fixture's one previous log: the log of the container that crashed."""
    logs: list[str] = [
        log
        for key, log in fixture.k8s["logs"].items()
        if key.endswith("/previous") and log is not None
    ]
    assert len(logs) == 1, f"{fixture.id}: expected one previous log"
    return logs[0]


def located(frames: list[Frame]) -> list[tuple[str, int, str | None, bool]]:
    """The frames as (file, line, function, is_app_frame)."""
    return [(f.file, f.line, f.function, f.is_app_frame) for f in frames]


@pytest.mark.parametrize(("fid", "family"), sorted(FIXTURE_FAMILIES.items()))
def test_first_app_frame_is_expected_code(fid, family):
    f = load_fixture(fid)
    code = f.expected["code"]
    log = crash_log(f)
    assert detect_families(log) == {family}
    frames = parse_frames(log)
    assert frames and all(frame.family == family for frame in frames)
    first = next(frame for frame in frames if frame.is_app_frame)
    assert points_at(first.file, code["path"]), f"the first app frame is in {first.file}"
    assert code["start_line"] <= first.line <= code["end_line"]


@pytest.mark.parametrize(
    ("parse", "text", "is_app_frame"),
    [
        # JVM: the class's package prefix.
        (jvm.parse, "\tat java.util.ArrayList.forEach(ArrayList.java:1596)", False),
        (jvm.parse, "\tat java.base/java.lang.Thread.run(Thread.java:1583)", False),
        (jvm.parse, "\tat javax.servlet.http.HttpServlet.service(HttpServlet.java:590)", False),
        (jvm.parse, "\tat jdk.internal.reflect.Method.invoke(Method.java:580)", False),
        (jvm.parse, "\tat sun.nio.ch.Net.connect(Net.java:579)", False),
        (jvm.parse, "\tat kotlin.collections.ArraysKt.first(_Arrays.kt:1054)", False),
        (jvm.parse, "\tat kotlinx.coroutines.DispatchedTask.run(DispatchedTask.kt:100)", False),
        (jvm.parse, "\tat scala.collection.immutable.List.map(List.scala:247)", False),
        (jvm.parse, "\tat akka.actor.ActorCell.invoke(ActorCell.scala:547)", False),
        (
            jvm.parse,
            "\tat org.springframework.boot.SpringApplication.run(SpringApplication.java:342)"
            " ~[spring-boot-3.5.7.jar:3.5.7]",
            False,
        ),
        (jvm.parse, "\tat org.apache.catalina.core.StandardWrapper.load(SW.java:10)", False),
        (jvm.parse, "\tat com.example.orders.OrderService.ship(OrderService.java:35)", True),
        # A prefix counts only at the start of the class name.
        (jvm.parse, "\tat com.example.java.Util.run(Util.java:9)", True),
        # Node: every core module (node:, ruling R55) and node_modules.
        (node.parse, "    at listOnTimeout (node:internal/timers:608:17)", False),
        (node.parse, "    at node:internal/main/run_main_module:28:49", False),
        (node.parse, "    at Redis.emit (node:events:518:28)", False),
        (node.parse, "    at Object.readFileSync (node:fs:441:20)", False),
        (node.parse, "    at load (/app/node_helpers/x.js:3:7)", True),
        (node.parse, "    at thing (/usr/src/app/src/node-thing.js:9:1)", True),
        (
            node.parse,
            "    at Parser.execute (/app/node_modules/redis-parser/parser.js:544:14)",
            False,
        ),
        (node.parse, "    at summarize (/usr/src/app/src/orders/summary.js:14:28)", True),
        # Python: site-packages, dist-packages, frozen modules and the standard library.
        (
            python.parse,
            '  File "/usr/local/lib/python3.13/site-packages/pydantic/main.py", line 792, in v',
            False,
        ),
        (python.parse, '  File "/usr/lib/python3/dist-packages/yaml/__init__.py", line 9', False),
        (python.parse, '  File "<frozen runpy>", line 198, in _run_module_as_main', False),
        (python.parse, '  File "/usr/lib/python3.12/json/decoder.py", line 353, in decode', False),
        (
            python.parse,
            '  File "/usr/local/lib/python3.13/asyncio/runners.py", line 44, in run',
            False,
        ),
        (python.parse, '  File "/app/orders/service.py", line 24, in _flatten_customer', True),
    ],
)
def test_library_frames_flagged(parse, text, is_app_frame):
    frames = parse(text)
    assert len(frames) == 1
    assert frames[0].is_app_frame is is_app_frame


@pytest.mark.parametrize(
    "text",
    [
        "\tat com.example.orders.Native.call(Native Method)",
        "\tat com.example.payouts.MainKt.main(Main.kt)",
        "\tat kotlinx.coroutines.BuildersKt.runBlocking(Unknown Source)",
    ],
)
def test_jvm_frames_without_a_line_are_skipped(text):
    assert jvm.parse(text) == []


def test_jvm_file_is_package_path_and_file_name():
    text = "\n".join(
        [
            "\tat com.example.payouts.MainKt.main(Main.kt:10)",
            "\tat com.example.payouts.MainKt$main$1.invokeSuspend(Main.kt:15)",
            "\tat com.example.shipping.RateCalculator.quoteAll$$anonfun$1(RateCalculator.scala:18)",
            "\tat com.example.shipping.Main$.main(Main.scala:16)",
            "\tat com.example.payouts.Payout$Companion.of(Payout.kt:7)",
            "\tat com.example.orders.OrdersApplication.lambda$run$0(OrdersApplication.java:17)",
            "\tat java.base/java.util.ArrayList$Itr.next(ArrayList.java:1052) ~[na:na]",
            "\tat Main.main(Main.java:5)",
        ]
    )
    assert located(jvm.parse(text)) == [
        ("com/example/payouts/Main.kt", 10, "com.example.payouts.MainKt.main", True),
        (
            "com/example/payouts/Main.kt",
            15,
            "com.example.payouts.MainKt$main$1.invokeSuspend",
            True,
        ),
        (
            "com/example/shipping/RateCalculator.scala",
            18,
            "com.example.shipping.RateCalculator.quoteAll$$anonfun$1",
            True,
        ),
        ("com/example/shipping/Main.scala", 16, "com.example.shipping.Main$.main", True),
        ("com/example/payouts/Payout.kt", 7, "com.example.payouts.Payout$Companion.of", True),
        (
            "com/example/orders/OrdersApplication.java",
            17,
            "com.example.orders.OrdersApplication.lambda$run$0",
            True,
        ),
        ("java/util/ArrayList.java", 1052, "java.util.ArrayList$Itr.next", False),
        ("Main.java", 5, "Main.main", True),
    ]


def test_jvm_deepest_cause_first_and_elided_frames_not_invented():
    text = "\n".join(
        [
            'Exception in thread "main" java.lang.RuntimeException: wrapped',
            "\tat com.example.App.run(App.java:30)",
            "\tat com.example.App.main(App.java:10)",
            "Caused by: java.lang.IllegalStateException: middle",
            "\tat com.example.Service.call(Service.java:20)",
            "\t... 2 more",
            "Caused by: java.lang.ArithmeticException: / by zero",
            "\tat com.example.Calc.divide(Calc.java:5)",
            "\tat com.example.Service.call(Service.java:18)",
            "\t... 3 more",
        ]
    )
    assert [(f.file, f.line) for f in jvm.parse(text)] == [
        ("com/example/Calc.java", 5),
        ("com/example/Service.java", 18),
        ("com/example/Service.java", 20),
        ("com/example/App.java", 30),
        ("com/example/App.java", 10),
    ]


def test_jvm_suppressed_frames_stay_with_their_exception():
    # A suppressed exception, and its own cause, are printed indented; only the
    # unindented "Caused by:" continues the main chain.
    text = "\n".join(
        [
            "java.lang.RuntimeException: outer",
            "\tat com.example.App.run(App.java:30)",
            "\tSuppressed: java.io.IOException: close failed",
            "\t\tat com.example.Res.close(Res.java:8)",
            "\t\t... 1 more",
            "\tCaused by: java.io.EOFException: eof",
            "\t\tat com.example.Res.flush(Res.java:12)",
            "\t\t... 2 more",
            "Caused by: java.lang.ArithmeticException: / by zero",
            "\tat com.example.Calc.divide(Calc.java:5)",
            "\t... 1 more",
        ]
    )
    assert [(f.file, f.line) for f in jvm.parse(text)] == [
        ("com/example/Calc.java", 5),
        ("com/example/App.java", 30),
        ("com/example/Res.java", 8),
        ("com/example/Res.java", 12),
    ]


def test_node_frame_formats():
    text = "\n".join(
        [
            "/app/dist/orders/service.js:30",
            "        amountCents: sign * event.charge.amount,",
            "                                         ^",
            "",
            "TypeError: Cannot read properties of undefined (reading 'amount')",
            "    at toLedgerEntry (/app/dist/orders/service.js:30:42)",
            "    at Array.map (<anonymous>)",
            "    at async SettlementService.flush (/app/dist/orders/service.js:48:34)",
            "    at /app/dist/main.js:31:27",
            "    at async /app/dist/main.js:40:5",
            "    at new Settlement (/app/dist/orders/settlement.js:12:11)",
            "    at Object.<anonymous> (/app/dist/main.js:3:1)",
            "    at file:///app/dist/esm/index.mjs:7:9",
            "    at process.processTicksAndRejections (node:internal/process/task_queues:105:5)",
        ]
    )
    assert located(node.parse(text)) == [
        ("/app/dist/orders/service.js", 30, "toLedgerEntry", True),
        ("/app/dist/orders/service.js", 48, "SettlementService.flush", True),
        ("/app/dist/main.js", 31, None, True),
        ("/app/dist/main.js", 40, None, True),
        ("/app/dist/orders/settlement.js", 12, "new Settlement", True),
        ("/app/dist/main.js", 3, "Object.<anonymous>", True),
        ("/app/dist/esm/index.mjs", 7, None, True),
        (
            "node:internal/process/task_queues",
            105,
            "process.processTicksAndRejections",
            False,
        ),
    ]


def test_python_chained_exceptions_last_traceback_first():
    text = "\n".join(
        [
            "Traceback (most recent call last):",
            '  File "/app/orders/service.py", line 24, in _flatten_customer',
            '    customer = data["customer"]',
            "               ~~~~^^^^^^^^^^^^",
            "KeyError: 'customer'",
            "",
            "During handling of the above exception, another exception occurred:",
            "",
            "Traceback (most recent call last):",
            '  File "/app/orders/worker.py", line 34, in <module>',
            "    main()",
            "    ~~~~^^",
            '  File "/app/orders/worker.py", line 23, in main',
            "    invoice = build_invoice(order)",
            '  File "/app/orders/service.py", line 40, in build_invoice',
            '    raise InvoiceError("bad order") from None',
            "orders.service.InvoiceError: bad order",
        ]
    )
    assert located(python.parse(text)) == [
        ("/app/orders/service.py", 40, "build_invoice", True),
        ("/app/orders/worker.py", 23, "main", True),
        ("/app/orders/worker.py", 34, "<module>", True),
        ("/app/orders/service.py", 24, "_flatten_customer", True),
    ]


def test_python_frame_without_function():
    text = '  File "/app/orders/config.py", line 3\n    x = = 1\n        ^\nSyntaxError: x'
    assert located(python.parse(text)) == [("/app/orders/config.py", 3, None, True)]


def test_families_are_complete_and_lowercase():
    assert [f.value for f in Family] == ["jvm", "node", "python", "go", "php", "ruby", "native"]


def test_frame_family_is_the_plain_value():
    (frame,) = python.parse('  File "/app/x.py", line 1, in f')
    assert frame.family == "python" and type(frame.family) is str


def test_no_trace_no_frames():
    text = "2026-11-02 10:01:12,481 INFO orders.worker started\nat 10:01 the queue was empty\n"
    assert detect_families(text) == set()
    assert parse_frames(text) == []
    assert parse_frames("") == []


def test_parse_frames_runs_each_detected_family_and_removes_duplicates():
    python_trace = "\n".join(
        [
            "Traceback (most recent call last):",
            '  File "/app/worker.py", line 34, in <module>',
            '  File "/app/service.py", line 24, in flatten',
            "KeyError: 'a'",
            "",
            "During handling of the above exception, another exception occurred:",
            "",
            "Traceback (most recent call last):",
            '  File "/app/worker.py", line 34, in <module>',
            '  File "/app/service.py", line 30, in build',
            "ValueError: b",
        ]
    )
    jvm_trace = "java.lang.IllegalStateException: c\n\tat com.example.App.run(App.java:9)"
    text = f"{python_trace}\n{jvm_trace}\n"
    assert detect_families(text) == {Family.JVM, Family.PYTHON}
    assert [(f.family, f.file, f.line) for f in parse_frames(text)] == [
        ("jvm", "com/example/App.java", 9),
        ("python", "/app/service.py", 30),
        ("python", "/app/worker.py", 34),
        ("python", "/app/service.py", 24),
    ]
