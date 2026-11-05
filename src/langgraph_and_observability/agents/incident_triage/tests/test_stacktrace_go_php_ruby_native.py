"""The Go, PHP, Ruby and native stack-trace parsers (spec §9.5, §9.6 item 2).

Each app incident's crash log leads to its expected code location: the first app frame
that ``parse_frames`` returns is in the expected file (``fixture_paths.points_at``) and
in the expected lines. The bare segfault prints no trace, so it gives no frames. Every
log of every fixture is detected as exactly the family of its trace, or as no family.
Small in-test traces cover each family's library rules and the format variants of the
runtimes.
"""

import pytest

from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace import (
    Family,
    detect_families,
    go,
    native,
    parse_frames,
    php,
    ruby,
)
from langgraph_and_observability.agents.incident_triage.fixtures.loader import (
    list_fixtures,
    load_fixture,
)
from langgraph_and_observability.agents.incident_triage.tests import (
    test_stacktrace_jvm_node_python as jvm_node_python,
)
from langgraph_and_observability.agents.incident_triage.tests.fixture_paths import points_at

# The fixture helpers of the JVM, Node and Python parser tests.
crash_log = jvm_node_python.crash_log
located = jvm_node_python.located

# The app incidents of these families, with the family of each crash log.
NEW_FIXTURE_FAMILIES = {
    "go-nil-map": Family.GO,
    "php-division-by-zero": Family.PHP,
    "ruby-nomethod": Family.RUBY,
    "cpp-out-of-range": Family.NATIVE,
}
# The family of every app incident's crash log.
CRASH_FAMILIES = jvm_node_python.FIXTURE_FAMILIES | NEW_FIXTURE_FAMILIES
# The app incident whose crash prints no trace.
NO_TRACE = "c-segfault"


def logs_with_families() -> list[pytest.ParameterSet]:
    """Every log of every fixture, with the families its trace is detected as.

    An app incident's crash log is its one previous log, and holds a trace of the
    incident's family. No other log (current logs, sidecars, the infrastructure
    incidents, the bare segfault) holds a trace.
    """
    params = []
    for fid in list_fixtures():
        for key, log in load_fixture(fid).k8s["logs"].items():
            if log is None:
                continue
            crashed = fid in CRASH_FAMILIES and key.endswith("/previous")
            families = {CRASH_FAMILIES[fid]} if crashed else set()
            params.append(pytest.param(log, families, id=f"{fid}:{key}"))
    return params


def go_trace(location: str) -> str:
    """A goroutine with one frame at ``location``."""
    return f"goroutine 1 [running]:\nmain.f()\n\t{location}\n"


def native_frame(position: str) -> str:
    """A symbolized sanitizer frame at ``position`` (``file:line[:column]``)."""
    return f"    #0 0x55f0c2a4b6d1 in f() {position}"


def test_every_app_incident_has_a_crash_family_or_prints_no_trace():
    apps = {fid for fid in list_fixtures() if load_fixture(fid).source is not None}
    assert apps == set(CRASH_FAMILIES) | {NO_TRACE}


@pytest.mark.parametrize(("log", "families"), logs_with_families())
def test_every_fixture_log_is_detected_as_its_family(log, families):
    assert detect_families(log) == families


@pytest.mark.parametrize(("fid", "family"), sorted(NEW_FIXTURE_FAMILIES.items()))
def test_first_app_frame_is_expected_code(fid, family):
    f = load_fixture(fid)
    code = f.expected["code"]
    frames = parse_frames(crash_log(f))
    assert frames and all(frame.family == family for frame in frames)
    first = next(frame for frame in frames if frame.is_app_frame)
    assert points_at(first.file, code["path"]), f"the first app frame is in {first.file}"
    assert code["start_line"] <= first.line <= code["end_line"]


def test_bare_segfault_yields_no_frames():
    log = crash_log(load_fixture(NO_TRACE))
    assert "Segmentation fault" in log
    assert detect_families(log) == set()
    assert parse_frames(log) == []


@pytest.mark.parametrize(
    ("parse", "text", "is_app_frame"),
    [
        # Go: the runtime, GOROOT, vendored packages and the module cache.
        (go.parse, go_trace("/usr/local/go/src/runtime/panic.go:791 +0x132"), False),
        (go.parse, go_trace("/usr/local/go/src/net/http/server.go:2220 +0x29"), False),
        (go.parse, go_trace("runtime/proc.go:283 +0x28"), False),  # a -trimpath build
        (go.parse, go_trace("/workspace/vendor/github.com/lib/pq/conn.go:90 +0x1f"), False),
        (go.parse, go_trace("vendor/github.com/lib/pq/conn.go:90"), False),
        (
            go.parse,
            go_trace("/go/pkg/mod/golang.org/x/sync@v0.17.0/errgroup/errgroup.go:93 +0x50"),
            False,
        ),
        (go.parse, go_trace("/root/go/pkg/mod/github.com/a/b@v1.2.0/c.go:7 +0x5"), False),
        # Ruling R57: a module version, a -trimpath standard-library path (its first
        # element has no dot) and the compiler's generated wrappers.
        (
            go.parse,
            go_trace("golang.org/x/sync@v0.17.0/errgroup/errgroup.go:93 +0x50"),
            False,
        ),
        (go.parse, go_trace("/home/ci/modcache/github.com/a/b@v1.2.0/c.go:7 +0x5"), False),
        (go.parse, go_trace("net/http/server.go:2220 +0x29"), False),
        (go.parse, go_trace("<autogenerated>:1 +0x2c"), False),
        (go.parse, go_trace("/workspace/internal/orders/service.go:53 +0x14c"), True),
        (go.parse, go_trace("example.com/orders/internal/orders/service.go:53 +0x14c"), True),
        # An absolute path counts only by the markers: GOROOT, vendor/ as a whole
        # directory, go/pkg/mod/ and a module version.
        (go.parse, go_trace("/workspace/internal/runtime/loop.go:9 +0x1"), True),
        (go.parse, go_trace("/workspace/internal/myvendor/client.go:9 +0x1"), True),
        (go.parse, go_trace("/workspace/net/http/client.go:9 +0x1"), True),
        (go.parse, go_trace("github.com/example/orders-api/cmd/api/main.go:37 +0x25"), True),
        # PHP: Composer's vendor directory.
        (php.parse, "#0 /var/www/html/vendor/monolog/monolog/src/Logger.php(3): f()", False),
        (php.parse, "#0 /var/www/html/src/Pricing/Rules.php(3): f()", True),
        # Ruby: gems, the standard library and core (<internal:...>).
        (
            ruby.parse,
            "\tfrom /usr/local/bundle/gems/rack-3.1.8/lib/rack/builder.rb:9:in 'Rack#call'",
            False,
        ),
        (
            ruby.parse,
            "\tfrom /usr/local/lib/ruby/3.4.0/net/http.rb:1600:in 'Net::HTTP#request'",
            False,
        ),
        (ruby.parse, "\tfrom <internal:kernel>:168:in 'Kernel#loop'", False),
        (ruby.parse, "\tfrom /usr/src/app/lib/fulfillment/x.rb:15:in 'X#render'", True),
        (ruby.parse, "\tfrom /app/lib/ruby_tools/x.rb:3:in 'X#y'", True),
        # Native: system headers and libraries, and the compiler and C runtimes.
        (native.parse, native_frame("/usr/include/c++/12/bits/stl_vector.h:1153"), False),
        (native.parse, native_frame("/usr/lib/llvm-18/include/c++/v1/vector:1539:12"), False),
        (native.parse, native_frame("/usr/local/include/boost/asio/io_context.hpp:5"), False),
        (native.parse, native_frame("/lib/gcc/x86_64-linux-gnu/12/include/x.h:5"), False),
        (
            native.parse,
            native_frame("../../../../src/libstdc++-v3/libsupc++/eh_throw.cc:98"),
            False,
        ),
        (
            native.parse,
            native_frame("../../../../src/libsanitizer/asan/asan_new_delete.cpp:164"),
            False,
        ),
        (
            native.parse,
            native_frame("/b/llvm/compiler-rt/lib/sanitizer_common/sanitizer_posix.cpp:9"),
            False,
        ),
        (native.parse, native_frame("nptl/pthread_kill.c:44"), False),
        (native.parse, native_frame("../sysdeps/posix/raise.c:26"), False),
        (native.parse, native_frame("./stdlib/abort.c:79"), False),
        (native.parse, native_frame("/build/glibc-SzIz7B/glibc-2.31/nptl/raise.c:51"), False),
        (native.parse, native_frame("/workspace/src/pick_planner.cpp:13"), True),
        # /lib/ counts only at the start; a relative app path is an app frame.
        (native.parse, native_frame("/workspace/lib/parse.c:9:3"), True),
        (native.parse, native_frame("src/main.cc:10:5"), True),
        (native.parse, native_frame("/usr/src/app/main.c:3"), True),
    ],
)
def test_library_frames_flagged(parse, text, is_app_frame):
    frames = parse(text)
    assert len(frames) == 1
    assert frames[0].is_app_frame is is_app_frame


def test_go_goroutine_dump_formats():
    text = "\n".join(
        [
            "panic: runtime error: index out of range [3] with length 3 [recovered]",
            "\tpanic: checkout failed",
            "[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x4a2b3c]",
            "",
            "goroutine 7 gp=0xc000102a80 m=3 mp=0xc000100008 [running]:",
            "panic({0x4a8f40?, 0x52c6a0?})",
            "\t/usr/local/go/src/runtime/panic.go:791 +0x132 fp=0xc0000 sp=0xc0001 pc=0x435a72",
            "main.(*Cart).Total(...)",
            "\t/app/cart.go:12",
            "main.checkout({{0xc0000a0000, 0x3}, 0x2}, 0x0)",
            "\t/app/main.go:30 +0x1d",
            "main.Map[...]({0xc000012345, 0x2, 0x2}, 0x4b2c10)",
            "\t/app/generic.go:8 +0x45",
            "...additional frames elided...",
            "created by main.main in goroutine 1",
            "\t/app/main.go:41 +0x8a",
            "",
            "goroutine 1 [chan receive, 3 minutes]:",
            "main.main()",
            "\t/app/main.go:44 +0x99",
            "",
            "goroutine 9 [select]:",
            "net/http.(*persistConn).writeLoop(0xc0001b6000)",
            "\t/usr/local/go/src/net/http/transport.go:2519 +0xe7",
            "created by net/http.(*Transport).dialConn",
            "\t/usr/local/go/src/net/http/transport.go:1875 +0x15a5",
            "exit status 2",
        ]
    )
    assert located(go.parse(text)) == [
        ("/usr/local/go/src/runtime/panic.go", 791, "panic", False),
        ("/app/cart.go", 12, "main.(*Cart).Total", True),
        ("/app/main.go", 30, "main.checkout", True),
        ("/app/generic.go", 8, "main.Map[...]", True),
        ("/app/main.go", 41, "main.main", True),
        ("/app/main.go", 44, "main.main", True),
        (
            "/usr/local/go/src/net/http/transport.go",
            2519,
            "net/http.(*persistConn).writeLoop",
            False,
        ),
        (
            "/usr/local/go/src/net/http/transport.go",
            1875,
            "net/http.(*Transport).dialConn",
            False,
        ),
    ]


def test_go_frames_only_inside_a_goroutine():
    text = "\n".join(
        [
            "worker.Run(0x1)",
            "\t/app/worker.go:9 +0x1",
            "goroutine 1 [running]:",
            "main.main()",
            "\t/app/main.go:5 +0x1d",
            "",
            "main.after()",
            "\t/app/after.go:3 +0x2",
        ]
    )
    assert located(go.parse(text)) == [("/app/main.go", 5, "main.main", True)]


def test_php_uncaught_exception_formats():
    # A frame's function is the one whose code is at its position: PHP prints, on each
    # #N line, the function called there, so a position is in the next line's function.
    text = "\n".join(
        [
            "[02-Nov-2026 12:30:04 UTC] PHP Fatal error:  Uncaught InvalidArgumentException:"
            " no lines in /app/src/Cart.php:40",
            "Stack trace:",
            "#0 /app/src/Checkout.php(12): App\\Cart->total()",
            "#1 [internal function]: App\\Checkout->run(Array)",
            "#2 /app/vendor/acme/queue/src/Worker.php(88): array_map(Object(Closure), Array)",
            "#3 /app/bin/worker.php(7): Acme\\Queue\\Worker->process(Array)",
            "#4 {main}",
            "",
            "Next RuntimeException: checkout failed in /app/src/Checkout.php:15",
            "Stack trace:",
            "#0 [internal function]: App\\Checkout->run(Array)",
            "#1 {main}",
            "  thrown in /app/src/Checkout.php on line 15",
        ]
    )
    assert located(php.parse(text)) == [
        ("/app/src/Cart.php", 40, "App\\Cart->total", True),
        ("/app/src/Checkout.php", 12, "App\\Checkout->run", True),
        ("/app/vendor/acme/queue/src/Worker.php", 88, "Acme\\Queue\\Worker->process", False),
        ("/app/bin/worker.php", 7, "{main}", True),
        ("/app/src/Checkout.php", 15, "App\\Checkout->run", True),
    ]


def test_php_internal_function_throw_is_one_frame():
    # intdiv threw: the header's position is #0's, the line that called intdiv.
    text = "\n".join(
        [
            "PHP Fatal error:  Uncaught DivisionByZeroError: Division by zero"
            " in /app/src/Calc.php:22",
            "Stack trace:",
            "#0 /app/src/Calc.php(22): intdiv(1500, 0)",
            "#1 /app/bin/worker.php(18): App\\Calc->apply(Array)",
            "#2 {main}",
            "  thrown in /app/src/Calc.php on line 22",
        ]
    )
    assert located(php.parse(text)) == [
        ("/app/src/Calc.php", 22, "App\\Calc->apply", True),
        ("/app/bin/worker.php", 18, "{main}", True),
    ]


def test_php_top_level_throw_and_closures():
    text = "\n".join(
        [
            "Fatal error: Uncaught Exception: boom in /app/index.php:3",
            "Stack trace:",
            "#0 {main}",
            "  thrown in /app/index.php on line 3",
            # A trace with no header, cut before {main}.
            "[stacktrace]",
            "#0 /app/src/Jobs.php(9): intdiv(1, 0)",
            "#1 [internal function]: App\\Jobs->{closure:App\\Jobs::run():8}(Array)",
            "#2 /app/src/Jobs.php(8): array_map(Object(Closure), Array)",
            "#3 /app/artisan(13): App\\Jobs->run()",
        ]
    )
    assert located(php.parse(text)) == [
        ("/app/index.php", 3, "{main}", True),
        ("/app/src/Jobs.php", 9, "App\\Jobs->{closure:App\\Jobs::run():8}", True),
        ("/app/src/Jobs.php", 8, "App\\Jobs->run", True),
        ("/app/artisan", 13, None, True),
    ]


def test_ruby_backtrace_formats():
    text = "\n".join(
        [
            "/app/lib/cart.rb:23:in 'Shop::Cart#total': undefined method 'price' for nil"
            " (NoMethodError)",
            "",
            "        items.sum { |i| i.price }",
            "                         ^^^^^^",
            "\tfrom /app/lib/cart.rb:23:in 'Array#sum'",
            "\tfrom <internal:kernel>:168:in 'Kernel#loop'",
            "\tfrom /usr/local/bundle/gems/sidekiq-7.3.0/lib/sidekiq/processor.rb:220:in"
            " 'block in Sidekiq::Processor#process'",
            "\t ... 5 levels...",
            "\tfrom /app/bin/worker.rb:12:in '<main>'",
            "/app/lib/label.rb:7:in `lines': undefined method `upcase' for nil:NilClass"
            " (NoMethodError)",
            "\tfrom /app/lib/label.rb:3:in `block in render'",
            "app/models/order.rb:12:in 'Order#total'",
        ]
    )
    assert located(ruby.parse(text)) == [
        ("/app/lib/cart.rb", 23, "Shop::Cart#total", True),
        ("/app/lib/cart.rb", 23, "Array#sum", True),
        ("<internal:kernel>", 168, "Kernel#loop", False),
        (
            "/usr/local/bundle/gems/sidekiq-7.3.0/lib/sidekiq/processor.rb",
            220,
            "block in Sidekiq::Processor#process",
            False,
        ),
        ("/app/bin/worker.rb", 12, "<main>", True),
        ("/app/lib/label.rb", 7, "lines", True),
        ("/app/lib/label.rb", 3, "block in render", True),
        ("app/models/order.rb", 12, "Order#total", True),
    ]


def test_ruby_causes_follow_in_print_order():
    text = "\n".join(
        [
            "/app/a.rb:5:in 'Object#b': outer (RuntimeError)",
            "\tfrom /app/a.rb:9:in '<main>'",
            "/app/a.rb:2:in 'Object#a': inner (ArgumentError)",
            "\tfrom /app/a.rb:4:in 'Object#b'",
        ]
    )
    assert [(f.file, f.line) for f in ruby.parse(text)] == [
        ("/app/a.rb", 5),
        ("/app/a.rb", 9),
        ("/app/a.rb", 2),
        ("/app/a.rb", 4),
    ]


def test_native_sanitizer_formats():
    text = "\n".join(
        [
            "==7==ERROR: AddressSanitizer: heap-use-after-free on address 0x602000000010"
            " at pc 0x55d4f1a2b3c4 bp 0x7ffc5c0a1f40 sp 0x7ffc5c0a1f38",
            "READ of size 4 at 0x602000000010 thread T0",
            # Three frames inlined at one PC.
            "    #0 0x55d4f1a2b3c4 in std::vector<int, std::allocator<int> >::operator[]"
            "(unsigned long) /usr/include/c++/13/bits/stl_vector.h:1128:25",
            "    #1 0x55d4f1a2b3c4 in Cart::total() const /src/cart.cpp:12:14",
            "    #2 0x55d4f1a2b3c4 in main /src/main.cpp:20:3",
            "    #3 0x7f1c2a229d8f in __libc_start_call_main"
            " ../sysdeps/nptl/libc_start_call_main.h:58:16",
            "    #4 0x7f1c2a229e3f in __libc_start_main csu/../csu/libc-start.c:392:3",
            "    #5 0x55d4f1a2b0e4 in _start (/usr/local/bin/cart+0x10e4) (BuildId: 1f2e3d4c)",
            "",
            "freed by thread T0 here:",
            "    #0 0x7f1c2a6b8a28 in operator delete(void*, unsigned long)"
            " ../../../../src/libsanitizer/asan/asan_new_delete.cpp:164",
            "    #1 0x55d4f1a2b111 in Cart::clear() /src/cart.cpp:30:5",
            "    #2 0x7f1c2a229d8f (/lib/x86_64-linux-gnu/libc.so.6+0x29d8f) (BuildId: 6d64b17f)",
            "",
            "SUMMARY: AddressSanitizer: heap-use-after-free /src/cart.cpp:12:14 in Cart::total()",
        ]
    )
    assert located(native.parse(text)) == [
        (
            "/usr/include/c++/13/bits/stl_vector.h",
            1128,
            "std::vector<int, std::allocator<int> >::operator[](unsigned long)",
            False,
        ),
        ("/src/cart.cpp", 12, "Cart::total() const", True),
        ("/src/main.cpp", 20, "main", True),
        ("../sysdeps/nptl/libc_start_call_main.h", 58, "__libc_start_call_main", False),
        ("csu/../csu/libc-start.c", 392, "__libc_start_main", False),
        (
            "../../../../src/libsanitizer/asan/asan_new_delete.cpp",
            164,
            "operator delete(void*, unsigned long)",
            False,
        ),
        ("/src/cart.cpp", 30, "Cart::clear()", True),
    ]


@pytest.mark.parametrize(
    "text",
    [
        "    #2 0x7f6d1e246249 (/lib/x86_64-linux-gnu/libc.so.6+0x29d90)",
        "    #14 0x55f0c2a49a10 in _start (/usr/local/bin/pick-planner+0x4a10)",
        "    #3 0x7f1c2a242520 in raise (/lib/x86_64-linux-gnu/libc.so.6+0x42520) (BuildId: 6d)",
        "    #2 0x4005a9 in process_batch",
    ],
)
def test_native_frames_without_a_file_and_line_are_skipped(text):
    assert native.parse(text) == []


@pytest.mark.parametrize(
    ("text", "family"),
    [
        (go_trace("/app/main.go:5 +0x1d"), Family.GO),
        ("#0 /app/src/Calc.php(22): intdiv(1500, 0)\n#1 {main}", Family.PHP),
        ("PHP Fatal error:  Uncaught Exception: boom in /app/index.php:3", Family.PHP),
        ("/app/lib/cart.rb:23:in 'Shop::Cart#total': boom (RuntimeError)", Family.RUBY),
        (native_frame("/src/cart.cpp:12:14"), Family.NATIVE),
    ],
)
def test_each_format_is_detected_as_its_family(text, family):
    assert detect_families(text) == {family}


@pytest.mark.parametrize(
    "text",
    [
        "goroutine 5 [running]:\n",
        "main.main()\n\t/app/main.go:5 +0x1d\n",
        "2026-11-02 ERROR Uncaught exception in /app/handler.py:42",
        "Next batch in /var/spool/orders:3",
        "SUMMARY: AddressSanitizer: heap-use-after-free /src/cart.cpp:12:14 in Cart::total()",
        "    #2 0x7f1c2a229d8f (/lib/x86_64-linux-gnu/libc.so.6+0x29d8f)",
        "Segmentation fault (core dumped)",
    ],
)
def test_near_misses_are_no_family(text):
    assert detect_families(text) == set()
    assert parse_frames(text) == []
