"""The agent's own fixtures: sample apps and incidents (spec §12.3, §13.10, FORMAT.md).

Every incident loads, and the simulated adapters accept its files. Each application
incident's crash log leads to the incident's expected code location: the first app
frame that ``parse_frames`` returns lies in the expected lines. A native crash that
prints no trace instead logs a message whose text is a literal in the expected lines.
"""

import re
from pathlib import Path

import pytest

from langgraph_postgres_starter.agents.incident_triage.adapters import build_adapters
from langgraph_postgres_starter.agents.incident_triage.codeintel.stacktrace import parse_frames
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import (
    Fixture,
    list_fixtures,
    load_fixture,
)
from langgraph_postgres_starter.agents.incident_triage.schemas import IncidentMessage
from langgraph_postgres_starter.agents.incident_triage.tests.fixture_paths import (
    compiled_path,
    points_at,
    same_file,
)
from langgraph_postgres_starter.settings import get_settings

CATEGORIES = {"infrastructure", "configuration", "application", "dependency", "unknown"}
# The category of each infrastructure incident. None of them has an app.
INFRA_CATEGORIES = {
    "infra-oomkilled": "infrastructure",
    "infra-imagepull": "configuration",
    "infra-probe-failure": "infrastructure",
    "infra-config-error": "configuration",
}
# The language of each source file, by extension.
LANGUAGES = {
    ".js": "javascript",
    ".ts": "typescript",
    ".py": "python",
    ".java": "java",
    ".go": "go",
    ".php": "php",
    ".rb": "ruby",
    ".kt": "kotlin",
    ".scala": "scala",
    ".c": "c",
    ".h": "c",
    ".cpp": "cpp",
    ".hpp": "cpp",
}
# The languages of spec §9.6.
SUPPORTED_LANGUAGES = {
    "javascript",
    "typescript",
    "python",
    "java",
    "go",
    "php",
    "ruby",
    "kotlin",
    "scala",
    "c",
    "cpp",
}
# Containers that run next to the app; a crash log is never theirs.
SIDECARS = {
    "istio-proxy",
    "linkerd-proxy",
    "datadog-agent",
    "envoy",
    "vault-agent",
    "cloud-sql-proxy",
}

# A log line's message: the text after its level.
_LOG_MESSAGE = re.compile(r"\b(?:DEBUG|INFO|WARN|WARNING|ERROR)\s+(?P<message>.+)$", re.MULTILINE)


def apps() -> list[Path]:
    """The sample apps' folders."""
    return sorted(p for p in (get_settings().fixtures_dir / "apps").iterdir() if p.is_dir())


def app_files(app: Path) -> list[Path]:
    """The files of a sample app."""
    return sorted(p for p in app.rglob("*") if p.is_file())


def language_of(app: Path) -> str:
    """The one language of a sample app's files."""
    suffixes = {p.suffix for p in app_files(app)}
    assert suffixes <= LANGUAGES.keys(), f"{app.name} has files that are not source files"
    languages = {LANGUAGES[suffix] for suffix in suffixes}
    assert len(languages) == 1, f"{app.name} mixes languages: {sorted(languages)}"
    return languages.pop()


def app_fixtures() -> list[str]:
    """The IDs of the incidents that have an app."""
    return [fid for fid in list_fixtures() if load_fixture(fid).source is not None]


def crash_log(fixture: Fixture) -> str:
    """The previous log of the app container whose last run ended with a non-zero exit."""
    logs: list[str | None] = [
        fixture.k8s["logs"].get(f"{pod['name']}/{container['name']}/previous")
        for pod in fixture.k8s["pods"]
        for container in pod["containers"]
        if container["name"] not in SIDECARS
        and container["last_terminated"] is not None
        and container["last_terminated"]["exit_code"] != 0
    ]
    assert len(logs) == 1, f"{fixture.id}: expected one crashed app container"
    log = logs[0]
    assert log, f"{fixture.id}: the crashed container has no previous log"
    return log


@pytest.mark.parametrize("fid", list_fixtures())
def test_fixture_valid(fid):
    f = load_fixture(fid)
    message = IncidentMessage.model_validate(f.incident)
    assert "fired_at" not in f.incident
    assert message.raw is not None and message.raw["fixture_id"] == fid
    # The fakes check k8s.json, datadog.json and source.json: a missing or unknown key fails.
    build_adapters(get_settings(), fixture_id=fid)
    assert set(f.expected) == {"category", "code", "facts"}
    assert f.expected["category"] in CATEGORIES
    facts = f.expected["facts"]
    assert 2 <= len(facts) <= 4 and all(isinstance(fact, str) and fact for fact in facts)
    code = f.expected["code"]
    if f.source is None:
        assert code is None, "an incident with no app has no code location"
        return
    assert set(code) == {"path", "start_line", "end_line", "symbol"}
    src = get_settings().fixtures_dir / "apps" / f.source["app"] / code["path"]
    assert src.is_file()
    lines = src.read_text().splitlines()
    assert 1 <= code["start_line"] <= code["end_line"] <= len(lines)
    assert code["symbol"] in lines[code["start_line"] - 1]


@pytest.mark.parametrize(("fid", "category"), sorted(INFRA_CATEGORIES.items()))
def test_infrastructure_incidents_have_no_app_signal(fid, category):
    f = load_fixture(fid)
    assert f.source is None
    assert f.expected["category"] == category
    # No stack trace, and every non-zero exit comes with an infrastructure reason.
    for log in f.k8s["logs"].values():
        assert log is None or parse_frames(log) == []
    probe_failed = any(e["reason"] == "Unhealthy" for e in f.k8s["events"])
    for pod in f.k8s["pods"]:
        for container in pod["containers"]:
            last = container["last_terminated"]
            if last is not None and last["exit_code"] != 0:
                assert last["reason"] == "OOMKilled" or probe_failed


def test_every_language_has_an_app():
    assert {language_of(app) for app in apps()} >= SUPPORTED_LANGUAGES


def test_apps_are_small():
    for app in apps():
        files = app_files(app)
        assert 1 <= len(files) <= 4, app.name
        assert sum(len(p.read_text().splitlines()) for p in files) <= 150, app.name


def test_every_app_has_an_incident():
    sources = [load_fixture(fid).source for fid in app_fixtures()]
    used = {source["app"] for source in sources if source is not None}
    assert used == {app.name for app in apps()}


@pytest.mark.parametrize("fid", app_fixtures())
def test_app_crash_logs_point_at_bug_line(fid):
    f = load_fixture(fid)
    assert f.source is not None
    code = f.expected["code"]
    app = get_settings().fixtures_dir / "apps" / f.source["app"]
    log = crash_log(f)
    app_frames = [frame for frame in parse_frames(log) if frame.is_app_frame]
    expected_lines = app.joinpath(code["path"]).read_text().splitlines()
    expected_lines = expected_lines[code["start_line"] - 1 : code["end_line"]]
    if app_frames:
        first = app_frames[0]
        assert points_at(first.file, code["path"]), f"the first app frame is in {first.file}"
        assert code["start_line"] <= first.line <= code["end_line"]
    else:
        # A native crash with no trace: the last logged message's text, up to its first
        # variable part, is a literal in the expected lines.
        messages = _LOG_MESSAGE.findall(log)
        assert messages, "a crash log with no trace logs messages"
        literal = re.split(r"\d", messages[-1], maxsplit=1)[0].strip()
        assert literal and any(literal in line for line in expected_lines)


@pytest.mark.parametrize(
    ("frame", "repo_path", "expected"),
    [
        ("/app/orders/service.py", "orders/service.py", True),
        ("orders/service.py", "/app/orders/service.py", True),
        (
            "com/example/orders/OrderController.java",
            "src/main/java/com/example/orders/OrderController.java",
            True,
        ),
        ("/app/dist/orders/service.js", "src/orders/service.ts", True),
        ("/app/src/orders/service.ts", "src/orders/service.ts", False),
        ("/usr/src/app/src/orders/summary.js", "src/orders/summary.js", True),
        ("/app/billing/service.py", "orders/service.py", False),
        ("/usr/local/lib/python3.13/site-packages/pydantic/main.py", "orders/main.py", False),
        ("", "orders/service.py", False),
    ],
)
def test_points_at(frame, repo_path, expected):
    assert points_at(frame, repo_path) is expected


def test_compiled_path_rewrites_only_typescript():
    assert compiled_path("src/orders/service.ts") == "dist/orders/service.js"
    assert compiled_path("src/ui/Cart.tsx") == "dist/ui/Cart.js"
    assert compiled_path("lib/orders/service.ts") == "lib/orders/service.js"
    assert compiled_path("src/orders/summary.js") == "src/orders/summary.js"
    assert same_file("a/b/c.py", "c.py")
