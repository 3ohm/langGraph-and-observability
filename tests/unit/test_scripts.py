"""``scripts/rename.py`` and ``scripts/remove_example.py`` on copies of the repository.

Each test copies the repository, without ``.git``, ``.venv`` and caches, into
``tmp_path`` and runs the copy's script there with this interpreter, so a script can
only ever change the copy. A pytest started in a copy imports the copy's package:
``PYTHONPATH`` puts the copy's ``src/`` before this environment's editable install.
Coverage is off in those processes, so they add nothing to this run's report.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

import langgraph_postgres_starter

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = langgraph_postgres_starter.__name__
DIST = PACKAGE.replace("_", "-")
NEW_NAME = "acme_triage_kit"
EXAMPLE_AGENT = "incident_triage"

_IGNORED = shutil.ignore_patterns(
    ".git",
    ".venv",
    ".superpowers",
    "__pycache__",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    ".langgraph_api",
    ".benchmarks",
    "htmlcov",
    ".coverage*",
)
# The build and config files that must not name the example agent once it is removed.
_BUILD_FILES = (
    "Makefile",
    "Dockerfile",
    "compose.yaml",
    "langgraph.json",
    "pyproject.toml",
    ".env.example",
)
_UNIT_TESTS = ["-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider"]
_UNIT_MARKERS = ["-m", "not integration and not e2e"]


@pytest.fixture
def repo_copy(tmp_path: Path) -> Path:
    copy = tmp_path / "repo"
    shutil.copytree(ROOT, copy, ignore=_IGNORED, symlinks=True)
    return copy


def _env(*python_path: Path) -> dict[str, str]:
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("COVERAGE_", "COV_CORE_"))
    }
    env["PYTHONPATH"] = os.pathsep.join(str(p) for p in python_path)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _run(
    cwd: Path, *args: str, python_path: tuple[Path, ...] = ()
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - this interpreter, with fixed arguments
        [sys.executable, *args],
        cwd=cwd,
        env=_env(*python_path),
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )


def _snapshot(root: Path) -> dict[str, str]:
    """Return a hash of every file below ``root``, by relative path; folders as ``dir``."""
    files: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        key = path.relative_to(root).as_posix()
        files[key] = "dir" if path.is_dir() else hashlib.sha256(path.read_bytes()).hexdigest()
    return files


def _files_containing(root: Path, text: str, *, skip: frozenset[str] = frozenset()) -> list[str]:
    """Return the text files below ``root`` that contain ``text`` (``grep -rl``)."""
    found = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name in skip:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if text in content:
            found.append(path.relative_to(root).as_posix())
    return found


def _fake_dist_info(site: Path, copy: Path) -> Path:
    """Make the renamed distribution's metadata findable, as ``uv sync`` would install it.

    The package reads its version from its distribution's metadata at import.
    """
    project = tomllib.loads((copy / "pyproject.toml").read_text())["project"]
    dist_info = site / f"{project['name'].replace('-', '_')}-{project['version']}.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.4\nName: {project['name']}\nVersion: {project['version']}\n"
    )
    return site


def test_rename_dry_run(repo_copy, tmp_path):
    # Ruling R69: --dry-run changes nothing and lists the changes; a real run renames.
    before = _snapshot(repo_copy)
    dry = _run(repo_copy, "scripts/rename.py", NEW_NAME, "--dry-run")
    assert dry.returncode == 0, dry.stderr
    assert _snapshot(repo_copy) == before
    assert "pyproject.toml" in dry.stdout
    assert f"src/{PACKAGE} -> src/{NEW_NAME}" in dry.stdout

    real = _run(repo_copy, "scripts/rename.py", NEW_NAME)
    assert real.returncode == 0, real.stderr
    assert not (repo_copy / "src" / PACKAGE).exists()
    assert (repo_copy / "src" / NEW_NAME / "__init__.py").is_file()
    assert _files_containing(repo_copy / "src", PACKAGE) == []
    # Nowhere else either, except LICENSE: MIT asks a copy to keep the copyright notice.
    for old in (PACKAGE, DIST):
        assert _files_containing(repo_copy, old, skip=frozenset({"LICENSE"})) == []
    assert _snapshot(repo_copy)["LICENSE"] == before["LICENSE"]

    site = _fake_dist_info(tmp_path / "site", repo_copy)
    collected = _run(repo_copy, *_UNIT_TESTS, *_UNIT_MARKERS, python_path=(repo_copy / "src", site))
    assert collected.returncode == 0, collected.stdout + collected.stderr


@pytest.mark.parametrize(
    "name", ["Acme", "1acme", "acme-kit", "_acme", "acme_", "acme__kit", "class", "json", PACKAGE]
)
def test_rename_rejects_a_bad_name(repo_copy, name):
    before = _snapshot(repo_copy)
    result = _run(repo_copy, "scripts/rename.py", name)
    assert result.returncode == 2
    assert result.stderr.startswith(f"rename.py: {name!r} ")
    assert _snapshot(repo_copy) == before


def test_remove_example_in_temp_copy(repo_copy):
    result = _run(repo_copy, "scripts/remove_example.py")
    assert result.returncode == 0, result.stderr
    package = repo_copy / "src" / PACKAGE
    assert not (package / "agents" / EXAMPLE_AGENT).exists()
    assert (package / "agents" / "__init__.py").is_file()
    assert EXAMPLE_AGENT not in json.loads((repo_copy / "langgraph.json").read_text())["graphs"]
    # The removal job's check: grep -r incident_triage src finds nothing.
    assert _files_containing(repo_copy / "src", EXAMPLE_AGENT) == []
    for name in _BUILD_FILES:
        assert EXAMPLE_AGENT not in (repo_copy / name).read_text(), name
    # CI names the agent only in the removal job's own check.
    workflows = [p for p in (repo_copy / ".github").rglob("*") if p.is_file()]
    naming = [line.strip() for p in workflows for line in p.read_text().splitlines()]
    assert [line for line in naming if EXAMPLE_AGENT in line] == [
        'run: "! grep -r incident_triage src"'
    ]

    # What is left imports cleanly: pytest collects the unit suite without an error.
    collected = _run(repo_copy, *_UNIT_TESTS, *_UNIT_MARKERS, python_path=(repo_copy / "src",))
    assert collected.returncode == 0, collected.stdout + collected.stderr


def test_remove_example_is_idempotent(repo_copy):
    first = _run(repo_copy, "scripts/remove_example.py")
    assert first.returncode == 0, first.stderr
    after_first = _snapshot(repo_copy)

    second = _run(repo_copy, "scripts/remove_example.py")
    assert second.returncode == 0, second.stderr
    assert _snapshot(repo_copy) == after_first
    assert "Nothing to change" in second.stdout


def test_remove_example_refuses_outside_a_repo_root(repo_copy, tmp_path):
    script = str(repo_copy / "scripts" / "remove_example.py")
    before = _snapshot(repo_copy)
    for cwd in (tmp_path, repo_copy / "src"):
        result = _run(cwd, script)
        assert result.returncode == 2, cwd
        assert "repository root" in result.stderr
    assert _snapshot(repo_copy) == before
