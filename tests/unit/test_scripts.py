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
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

import langgraph_and_observability

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = langgraph_and_observability.__name__
DIST = PACKAGE.replace("_", "-")
NEW_NAME = "acme_triage_kit"
# The template's own names, built from parts so that rename.py does not rewrite them here:
# the template's diffs hold these names, whatever a fork renamed its package to. When the
# template itself is renamed, change these parts and the brackets in docs/guides/upgrade.md.
TEMPLATE_PACKAGE = "_".join(("langgraph", "and", "observability"))
TEMPLATE_DIST = TEMPLATE_PACKAGE.replace("_", "-")
UPGRADE_GUIDE = Path("docs", "guides", "upgrade.md")
EXAMPLE_AGENT = "incident_triage"
# A first removal needs the example agent; the removal job runs this suite without it.
needs_example_agent = pytest.mark.skipif(
    not (Path(langgraph_and_observability.__file__).parent / "agents" / EXAMPLE_AGENT).is_dir(),
    reason="the example agent was removed",
)

_IGNORED = shutil.ignore_patterns(  # pylint: disable=invalid-name  # a module constant
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
# The variables that a make running this suite (make test) passes to its children.
_MAKE_VARIABLES = ("MAKEFLAGS", "MFLAGS", "MAKELEVEL", "MAKEOVERRIDES")


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


def _dangling_links(root: Path) -> list[str]:
    """Return each Markdown link below ``root`` to a file that does not exist, as file: link."""
    dangling = []
    for doc in sorted(root.rglob("*.md")):
        for link in re.findall(r"\]\(([^)\s]+)\)", doc.read_text(encoding="utf-8")):
            if re.match(r"[a-z]+:|#", link):
                continue
            if not (doc.parent / link.partition("#")[0]).exists():
                dangling.append(f"{doc.relative_to(root).as_posix()}: {link}")
    return dangling


def _make_targets(root: Path) -> list[str]:
    """Return the targets that the Makefile defines and that the CI workflows call."""
    defined = re.findall(r"^([a-z0-9][a-z0-9_.-]*):(?!=)", (root / "Makefile").read_text(), re.M)
    called = []
    for workflow in sorted((root / ".github" / "workflows").glob("*.yml")):
        jobs = yaml.safe_load(workflow.read_text())["jobs"].values()
        for line in (
            line for job in jobs for s in job["steps"] for line in s.get("run", "").splitlines()
        ):
            for words in re.findall(r"\bmake\s+([^&|;#]*)", line):
                called += [w for w in words.split() if "=" not in w and not w.startswith("-")]
    return sorted(set(defined) | set(called))


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
    "name",
    [
        "Acme",
        "1acme",
        "acme-kit",
        "_acme",
        "acme_",
        "acme__kit",
        "class",
        "json",  # a standard-library module
        PACKAGE,  # the current name
        "langgraph",  # a package in uv.lock: the lock would hold two of that name
        "psycopg_pool",  # in uv.lock as psycopg-pool
        "yaml",  # importable already (PyYAML), though uv.lock names it pyyaml
    ],
)
def test_rename_rejects_a_bad_name(repo_copy, name):
    before = _snapshot(repo_copy)
    result = _run(repo_copy, "scripts/rename.py", name)
    assert result.returncode == 2
    assert result.stderr.startswith(f"rename.py: {name!r} ")
    assert _snapshot(repo_copy) == before


def _diff_lines(package: str, dist: str) -> str:
    """Return lines of a template diff that hold a package path and a Kubernetes name."""
    return (
        f"--- a/src/{package}/worker/runs.py\n"
        f"+++ b/src/{package}/worker/runs.py\n"
        f"+  namespace: {dist}\n"
    )


def _upgrade_sed(guide: Path, text: str) -> str:
    """Run the upgrade guide's ``sed`` expressions on ``text``, and return the output."""
    expressions = re.findall(r"-e '([^']+)'", guide.read_text())
    assert len(expressions) == 2, expressions
    result = subprocess.run(  # noqa: S603 - sed from PATH, with the guide's expressions
        ["sed", *(arg for expression in expressions for arg in ("-e", expression))],  # noqa: S607
        input=text,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return result.stdout


@pytest.mark.parametrize("renamed", [False, True], ids=["as-is", "renamed"])
def test_upgrade_guide_sed_survives_rename(tmp_path, renamed):
    """The upgrade guide's sed turns the template's names into the fork's, also after rename.py.

    rename.py rewrites every name in the docs. The guide's patterns must keep matching the
    template's names, and its replacements must become the fork's names.
    """
    copy = tmp_path / "repo"
    (copy / "src" / PACKAGE).mkdir(parents=True)
    (copy / "pyproject.toml").write_text(f'[project]\nname = "{DIST}"\n')
    (copy / "scripts").mkdir()
    shutil.copy2(ROOT / "scripts" / "rename.py", copy / "scripts")
    (copy / UPGRADE_GUIDE).parent.mkdir(parents=True)
    shutil.copy2(ROOT / UPGRADE_GUIDE, copy / UPGRADE_GUIDE)
    package = PACKAGE
    if renamed:
        result = _run(copy, "scripts/rename.py", NEW_NAME)
        assert result.returncode == 0, result.stderr
        assert str(UPGRADE_GUIDE) in result.stdout
        package = NEW_NAME

    ported = _upgrade_sed(copy / UPGRADE_GUIDE, _diff_lines(TEMPLATE_PACKAGE, TEMPLATE_DIST))
    assert ported == _diff_lines(package, package.replace("_", "-"))


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
    # The real-mode guide describes only the example's adapters; the docs drop its links.
    assert not (repo_copy / "docs" / "guides" / "real-mode.md").exists()
    assert "real-mode.md" not in (repo_copy / "README.md").read_text()
    assert _dangling_links(repo_copy) == []
    # CI names the agent only in the removal job's own check.
    workflows = [p for p in (repo_copy / ".github").rglob("*") if p.is_file()]
    naming = [line.strip() for p in workflows for line in p.read_text().splitlines()]
    assert [line for line in naming if EXAMPLE_AGENT in line] == [
        'run: "! grep -r incident_triage src"'
    ]

    # What is left imports cleanly: pytest collects the unit suite without an error.
    collected = _run(repo_copy, *_UNIT_TESTS, *_UNIT_MARKERS, python_path=(repo_copy / "src",))
    assert collected.returncode == 0, collected.stdout + collected.stderr

    # No recipe or CI step calls a make target that the removal deleted: each target that
    # the Makefile defines or CI calls dry-runs, with the $(MAKE) lines of its recipe.
    env = {k: v for k, v in os.environ.items() if k not in _MAKE_VARIABLES}
    targets = _make_targets(repo_copy)
    assert {"setup", "lint", "typecheck", "test"} <= set(targets)
    for target in targets:
        dry_run = subprocess.run(  # noqa: S603 - make from PATH, with fixed arguments
            ["make", "-n", target],  # noqa: S607
            cwd=repo_copy,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert dry_run.returncode == 0, f"make -n {target}: {dry_run.stderr}"


def test_remove_example_without_the_worker(repo_copy):
    # Spec 13.9: a fork that deleted the worker can still remove the example. The
    # registry edits go with the worker; everything else is removed as before.
    worker = repo_copy / "src" / PACKAGE / "worker"
    if worker.is_dir():  # a fork without the worker runs this suite too
        shutil.rmtree(worker)

    result = _run(repo_copy, "scripts/remove_example.py")

    assert result.returncode == 0, result.stderr
    assert not (repo_copy / "src" / PACKAGE / "agents" / EXAMPLE_AGENT).exists()
    assert _files_containing(repo_copy / "src", EXAMPLE_AGENT) == []


def test_remove_example_is_idempotent(repo_copy):
    first = _run(repo_copy, "scripts/remove_example.py")
    assert first.returncode == 0, first.stderr
    after_first = _snapshot(repo_copy)

    second = _run(repo_copy, "scripts/remove_example.py")
    assert second.returncode == 0, second.stderr
    assert _snapshot(repo_copy) == after_first
    assert "Nothing to change" in second.stdout


@needs_example_agent
@pytest.mark.parametrize(
    ("path", "old", "new"),
    [
        ("Makefile", "\t$(MAKE) grammars\n", "\t@$(MAKE) grammars\n"),
        (".github/workflows/ci.yml", "- run: make grammars\n", "- run: make grammars # fetch\n"),
    ],
    ids=["makefile-setup-recipe", "ci-grammars-step"],
)
def test_remove_example_refuses_a_drifted_edit_on_the_first_run(repo_copy, path, old, new):
    # A text that the removal must edit has changed: the first run changes nothing.
    target = repo_copy / path
    assert old in target.read_text()
    target.write_text(target.read_text().replace(old, new))
    before = _snapshot(repo_copy)

    result = _run(repo_copy, "scripts/remove_example.py")
    assert result.returncode == 1
    assert path in result.stderr and "nothing was changed" in result.stderr
    assert _snapshot(repo_copy) == before


@needs_example_agent
@pytest.mark.parametrize(
    ("path", "addition"),
    [
        ("Makefile", "\nci: ## lint, then fetch the grammars\n\t$(MAKE) lint grammars\n"),
        (".github/workflows/ci.yml", "      - run: make grammars lint\n"),
    ],
    ids=["makefile-recipe", "ci-step"],
)
def test_remove_example_refuses_to_leave_a_call_to_a_removed_target(repo_copy, path, addition):
    # The marked part deletes the grammars target; a call that nothing else removes remains.
    target = repo_copy / path
    target.write_text(target.read_text() + addition)
    before = _snapshot(repo_copy)

    result = _run(repo_copy, "scripts/remove_example.py")
    assert result.returncode == 1
    assert f"{path}: make grammars" in result.stderr and "nothing was changed" in result.stderr
    assert _snapshot(repo_copy) == before


@pytest.mark.parametrize(
    ("path", "addition"),
    [
        (".github/workflows/ci.yml", "      - run: make ${{ matrix.target }}\n"),
        ("Makefile", "\nlint-sub: ## lint a subproject\n\tmake -C sub lint\n"),
        ("Makefile", "\nquiet: ## lint, stderr to stdout\n\tmake lint 2>&1\n"),
        ("Makefile", "\nany: ## the target in TARGET\n\t$(MAKE) $(TARGET)\n"),
    ],
    ids=["ci-matrix-target", "make-C-folder", "redirect", "variable-target"],
)
def test_remove_example_allows_make_calls_it_does_not_break(repo_copy, path, addition):
    # Only a call to a target that the removal itself deletes is refused. These words are
    # no such target: an expression, a folder, a redirect and a variable.
    target = repo_copy / path
    target.write_text(target.read_text() + addition)

    result = _run(repo_copy, "scripts/remove_example.py")

    assert result.returncode == 0, result.stderr
    assert addition in target.read_text()


def test_remove_example_refuses_outside_a_repo_root(repo_copy, tmp_path):
    script = str(repo_copy / "scripts" / "remove_example.py")
    before = _snapshot(repo_copy)
    for cwd in (tmp_path, repo_copy / "src"):
        result = _run(cwd, script)
        assert result.returncode == 2, cwd
        assert "repository root" in result.stderr
    assert _snapshot(repo_copy) == before
