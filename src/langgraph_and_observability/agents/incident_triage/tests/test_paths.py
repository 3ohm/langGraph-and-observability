"""Mapping stack-frame files to repository files (spec §9.6 item 3).

The parametrized cases use the agent's sample apps; the other tests build small trees
under ``tmp_path``.
"""

import os
from pathlib import Path

import pytest

from langgraph_and_observability.agents.incident_triage.codeintel.paths import FileIndex
from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace import parse_frames
from langgraph_and_observability.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_and_observability.agents.incident_triage.schemas import Frame
from langgraph_and_observability.agents.incident_triage.tests.test_fixtures_valid import (
    app_fixtures,
    crash_log,
)
from langgraph_and_observability.settings import load_settings

APPS = load_settings().fixtures_dir / "apps"


def _frame(file: str, family: str, function: str | None = None) -> Frame:
    return Frame(file=file, line=1, function=function, is_app_frame=True, family=family)


def _tree(root: Path, *paths: str) -> Path:
    for path in paths:
        file = root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text("x\n")
    return root


@pytest.mark.parametrize(
    ("app", "frame", "expected"),
    [
        (
            "java-spring",
            _frame(
                "com/example/orders/OrderController.java",
                "jvm",
                "com.example.orders.OrderController.create",
            ),
            "src/main/java/com/example/orders/OrderController.java",
        ),
        ("python", _frame("/app/orders/service.py", "python", "settle"), "orders/service.py"),
        (
            "typescript-node",
            _frame("/app/dist/orders/service.js", "node", "toLedgerEntry"),
            "src/orders/service.ts",
        ),
        ("python", _frame("/app/billing/ledger.py", "python", "post"), None),
    ],
    ids=["java-spring", "python", "typescript", "unknown"],
)
def test_suffix_resolution(app, frame, expected):
    assert FileIndex.build(APPS / app).resolve(frame) == expected


def _app_frames(fid: str) -> list[Frame]:
    return [f for f in parse_frames(crash_log(load_fixture(fid))) if f.is_app_frame]


# A native crash that prints no trace has no frame to resolve.
@pytest.mark.parametrize("fid", [fid for fid in app_fixtures() if _app_frames(fid)])
def test_first_app_frame_of_each_fixture_resolves_to_its_expected_file(fid):
    fixture = load_fixture(fid)
    assert fixture.source is not None
    app_frames = _app_frames(fid)
    index = FileIndex.build(APPS / fixture.source["app"])

    assert index.resolve(app_frames[0]) == fixture.expected["code"]["path"]


def test_longest_suffix_wins_and_ties_go_to_the_shortest_path(tmp_path):
    index = FileIndex.build(
        _tree(
            tmp_path,
            "orders/service.py",
            "billing/service.py",
            "legacy/billing/service.py",
            "tools/vendor/billing/service.py",
            "a/b/util.py",
            "c/util.py",
        )
    )

    assert index.resolve(_frame("/app/billing/service.py", "python")) == "billing/service.py"
    assert index.resolve(_frame("/srv/orders/service.py", "python")) == "orders/service.py"
    # Only the file name matches: the shortest of the paths with that name.
    assert index.resolve(_frame("/app/util.py", "python")) == "c/util.py"
    assert index.resolve(_frame("/app/payments/service.py", "python")) == "orders/service.py"


def test_suffixes_match_whole_path_components(tmp_path):
    index = FileIndex.build(_tree(tmp_path, "orders/service.py"))

    assert index.resolve(_frame("/app/orders/xservice.py", "python")) is None
    assert index.resolve(_frame("/app/orders/service.pyc", "python")) is None
    assert index.resolve(_frame("orders/service.py", "python")) == "orders/service.py"
    assert index.resolve(_frame("./orders//service.py", "python")) == "orders/service.py"
    assert index.resolve(_frame("", "python")) is None
    assert index.resolve(_frame("/", "python")) is None


def test_jvm_frames_map_the_package_path(tmp_path):
    index = FileIndex.build(
        _tree(
            tmp_path,
            "orders/src/main/java/com/example/orders/Main.java",
            "billing/src/main/java/com/example/billing/Main.java",
        )
    )

    # The parsers write a JVM frame's file as its package path.
    billing = _frame("com/example/billing/Main.java", "jvm", "com.example.billing.Main.main")
    assert index.resolve(billing) == "billing/src/main/java/com/example/billing/Main.java"
    # A frame that names only the file: the package comes from the class in the function.
    bare = _frame("Main.java", "jvm", "com.example.billing.Main$Inner.run")
    assert index.resolve(bare) == "billing/src/main/java/com/example/billing/Main.java"
    # Without a function, only the file name is known: the shortest path wins.
    no_function = _frame("Main.java", "jvm", None)
    assert index.resolve(no_function) == "orders/src/main/java/com/example/orders/Main.java"
    assert index.resolve(_frame("Missing.java", "jvm", "com.example.Missing.run")) is None


def test_js_frames_fall_back_to_typescript_sources(tmp_path):
    index = FileIndex.build(
        _tree(tmp_path, "src/ui/Cart.tsx", "src/orders/service.ts", "scripts/service.js")
    )

    assert index.resolve(_frame("/app/dist/ui/Cart.js", "node", None)) == "src/ui/Cart.tsx"
    # The fallback's longer match beats the compiled name's match on the file name alone.
    assert index.resolve(_frame("/app/dist/orders/service.js", "node")) == "src/orders/service.ts"
    # Only Node frames get the fallback.
    assert index.resolve(_frame("/app/dist/ui/Cart.js", "python")) is None


def test_js_frames_prefer_a_committed_compiled_file(tmp_path):
    index = FileIndex.build(_tree(tmp_path, "src/orders/service.ts", "dist/orders/service.js"))

    assert index.resolve(_frame("/app/dist/orders/service.js", "node")) == "dist/orders/service.js"


def test_build_skips_git_and_symlinks(tmp_path):
    _tree(tmp_path, "orders/service.py", ".git/objects/service.py", ".git/HEAD")
    outside = _tree(tmp_path / "outside", "secret.py") / "secret.py"
    (tmp_path / "repo").mkdir()
    os.symlink(outside, tmp_path / "repo" / "secret.py")
    os.symlink(tmp_path / "outside", tmp_path / "repo" / "linked")

    index = FileIndex.build(tmp_path / "repo")

    assert index.file_count == 0
    assert index.resolve(_frame("/app/secret.py", "python")) is None
    full = FileIndex.build(tmp_path)
    assert full.resolve(_frame("/app/objects/service.py", "python")) == "orders/service.py"
    assert full.resolve(_frame("HEAD", "python")) is None


def test_build_caps_the_file_count(tmp_path):
    _tree(tmp_path, "a.py", "b.py", "c/d.py", "c/e.py")

    capped = FileIndex.build(tmp_path, max_files=3)
    full = FileIndex.build(tmp_path)

    assert (capped.file_count, capped.truncated) == (3, True)
    assert (full.file_count, full.truncated) == (4, False)
    # The walk is in name order, so the cap drops the same file every time.
    assert capped.resolve(_frame("e.py", "python")) is None
    assert capped.resolve(_frame("d.py", "python")) == "c/d.py"
