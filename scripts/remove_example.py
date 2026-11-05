"""Remove the example agent, Incident Triage, from a fork (spec §13.9; P22).

Run it from the repository root::

    uv run scripts/remove_example.py

It deletes the agent's package, ``src/<package>/agents/incident_triage/``, and the files
that only the example uses: ``scripts/send_incident.py`` (``make incident``) and
``scripts/sync_dataset.py``, their tests, the agent's end-to-end test, the Evals
workflow and the real-mode guide, whose links it removes from ``README.md``,
``docs/guides/quickstart.md``, ``docs/guides/run-on-eks.md`` and
``docs/release-checklist.md``. It removes
the agent's entry from ``langgraph.json`` and its line from ``worker/registry.py`` (in a
fork that has deleted the worker, the registry is gone with it), and
it resets the settings default that names it (``intake_agent`` to ``""``) in
``settings.py``, in its test and in ``.env.example``. It also removes the example's
parts of the build and of the settings: the ``grammars`` and ``incident`` make targets,
the Dockerfile's tree-sitter grammars, the CI steps that fetch them, and the example's
settings fields with their ``.env.example`` lines, their test defaults and their keys,
secrets and source cache in ``deploy/k8s/``. A file marks such a part with a
``# example-agent:begin`` line and a ``# example-agent:end`` line; the part goes with
the lines between them, and the blank lines after it.

The example's dependencies (``EXAMPLE_DEPENDENCIES``) stay in ``pyproject.toml`` and
``uv.lock``: the script prints the ``uv remove`` command for them as the next step. JSON
has no comments, so ``deploy/iam/policy.json`` keeps the example's Bedrock and ECR
statements.

The script works out every change before it makes one, then prints each change as it
makes it. It refuses, and changes nothing:

- outside the repository root;
- on a first run (the agent's package is still there), when a file it must change has
  drifted: an edit's text is not there, or a marked file has no marked part. Change the
  script to match the file, or the file to match the script;
- when, after the removal, a Makefile recipe or a CI step would still call a make target
  that the removal deletes (``$(MAKE) grammars``, ``make grammars``). Other words after
  ``make``, such as ``-C folder``, ``$(TARGET)``, ``${{ matrix.target }}`` or a redirect,
  are never refused.

After a first run, the script changes only what still holds the example's text, so a
second run changes nothing. It imports nothing from the package and uses the standard
library only.

Afterwards the docs (``README.md``, ``AGENTS.md``, ``docs/``) still describe the
example: update them for your fork. ``docs/guides/customize.md`` shows how to add your
own agent.
"""

import argparse
import json
import re
import shutil
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

PROG = "remove_example.py"
PACKAGE = "langgraph_and_observability"
AGENT = "incident_triage"
PACKAGE_DIR = f"src/{PACKAGE}"
# A fork may delete the worker (spec 13.9); the edits of its files go with it.
WORKER_DIR = f"{PACKAGE_DIR}/worker"

# The files and folders that only the example agent uses.
DELETE = (
    f"{PACKAGE_DIR}/agents/{AGENT}",
    "scripts/send_incident.py",
    "scripts/sync_dataset.py",
    "tests/unit/test_send_incident.py",
    "tests/unit/test_sync_dataset.py",
    "tests/e2e/test_triage_e2e.py",
    ".github/workflows/evals.yml",
    "docs/guides/real-mode.md",
)

# The lines that open and close a part of a file that only the example agent uses.
BEGIN = "# example-agent:begin"
END = "# example-agent:end"
MARKED_FILES = (
    "Makefile",
    "Dockerfile",
    "tests/unit/test_ci_config.py",
    f"{PACKAGE_DIR}/settings.py",
    ".env.example",
    "tests/unit/test_settings.py",
    "deploy/k8s/configmap.yaml",
    "deploy/k8s/deployment.yaml",
    "tests/unit/test_manifests.py",
)
# The project dependencies that only the example agent uses. The script leaves
# pyproject.toml and uv.lock as they are, and prints the uv remove command for these.
EXAMPLE_DEPENDENCIES = (
    "kubernetes-asyncio",
    "datadog-api-client",
    "tree-sitter",
    "tree-sitter-language-pack",
    "aiohttp",
)


@dataclass(frozen=True)
class Edit:
    """Replace every ``old`` in the file ``path`` with ``new``."""

    path: str
    old: str
    new: str = ""


EDITS = (
    # The registry line, and the import that it uses.
    Edit(
        f"{PACKAGE_DIR}/worker/registry.py",
        f"from {PACKAGE}.agents.{AGENT}.spec import spec as {AGENT}_spec\n",
    ),
    Edit(
        f"{PACKAGE_DIR}/worker/registry.py",
        f"    return [\n        {AGENT}_spec,\n    ]\n",
        "    return []\n",
    ),
    # The settings default that names the agent (ruling R29), with its test and docs.
    Edit(f"{PACKAGE_DIR}/settings.py", f'intake_agent: str = "{AGENT}"', 'intake_agent: str = ""'),
    Edit("tests/unit/test_settings.py", f'"intake_agent": "{AGENT}"', '"intake_agent": ""'),
    Edit(".env.example", f"APP_INTAKE_AGENT={AGENT}\n", "APP_INTAKE_AGENT=\n"),
    # The imports that only the marked parts of these files used. A marked part between
    # two imports would leave a blank line there, which ruff's import sort rejects.
    Edit(f"{PACKAGE_DIR}/settings.py", "from pathlib import Path\n"),
    Edit("tests/unit/test_settings.py", f"import {PACKAGE}\n"),
    # make setup no longer fetches the grammars; CI no longer either.
    Edit(
        "Makefile",
        "every dependency group into .venv, then fetch the grammars\n"
        "\tuv sync --all-groups\n"
        "\t$(MAKE) grammars\n",
        "every dependency group into .venv\n\tuv sync --all-groups\n",
    ),
    Edit(".github/workflows/ci.yml", "      - run: make grammars\n"),
    Edit(
        ".github/workflows/ci.yml",
        "# A job that runs tests first fetches the tree-sitter grammars (make grammars): "
        "tests never\n# download one.\n",
    ),
    Edit(
        ".github/workflows/ci.yml",
        "  # make test-e2e (pytest -m e2e) also runs tests/e2e/test_triage_e2e.py: the\n"
        f"  # {AGENT} agent in simulated mode, with every fixture incident and a kill -9\n"
        "  # during its Datadog step.\n",
    ),
    # The Evals workflow's tests, a marked part of the file, were the only users of these.
    Edit("tests/unit/test_ci_config.py", "from typing import Any, cast\n"),
    Edit(
        "pyproject.toml",
        ", with LangSmith tracing and an incident-triage example agent.",
        ", with LangSmith tracing.",
    ),
    # The links to the deleted real-mode guide.
    Edit(
        "README.md",
        "- [docs/guides/real-mode.md](docs/guides/real-mode.md): eight steps to a first real "
        "triage, each\n  real adapter (Kubernetes, Datadog, Bedrock, Git and ECR, SQS) and its "
        "settings.\n",
    ),
    Edit(
        "docs/guides/quickstart.md",
        "- [real-mode.md](real-mode.md): each real adapter and its settings, and a first real "
        "triage.\n",
    ),
    Edit(
        "docs/guides/run-on-eks.md",
        " Each adapter's settings, and the steps to a\nfirst real triage, are in "
        "[real-mode.md](real-mode.md); this guide is about the deployment.\n",
        "\n",
    ),
    Edit(
        "docs/release-checklist.md",
        "Follow [guides/real-mode.md](guides/real-mode.md), steps 1 to 8, with the owner's AWS, "
        "EKS,\n",
        "Run a first real triage of your agent, with the owner's AWS, EKS,\n",
    ),
)


class RemovalError(Exception):
    """A file is not in the shape that the removal expects; nothing was changed."""


def remove_marked_parts(text: str) -> str:
    """Return ``text`` without its marked parts, nor the blank lines after each part.

    Raises RemovalError for a part that has no end line, or an end line with no start.
    """
    kept: list[str] = []
    inside = after_part = False
    for line in text.splitlines(keepends=True):
        marker = line.strip()
        if inside:
            inside = not marker.startswith(END)
            after_part = not inside
        elif marker.startswith(BEGIN):
            inside = True
        elif marker.startswith(END):
            raise RemovalError(f"{END!r} without {BEGIN!r}")
        elif not (after_part and not marker):
            after_part = False
            kept.append(line)
    if inside:
        raise RemovalError(f"{BEGIN!r} without {END!r}")
    result = "".join(kept)
    # A part at the end of the file leaves blank lines there.
    if result != text and text.endswith("\n"):
        result = result.rstrip("\n") + "\n"
    return result


def _without_example_graph(text: str) -> str:
    """Return ``langgraph.json`` without the example agent's graph, in the same layout."""
    config = json.loads(text)
    graphs = config.get("graphs", {})
    if AGENT not in graphs:
        return text
    del graphs[AGENT]
    indent = 2 if text.strip().count("\n") else None
    return json.dumps(config, indent=indent) + "\n"


def make_targets(makefile: str) -> set[str]:
    """Return the targets that the rules of a Makefile define (variables are not rules)."""
    targets: set[str] = set()
    for line in makefile.splitlines():
        if line.startswith(("\t", "#")) or ":" not in line:
            continue
        head, _, rest = line.partition(":")
        # NAME := value, and NAME = value with a colon in the value.
        if not rest.startswith("=") and "=" not in head:
            targets.update(head.split())
    return targets


def make_calls(text: str, *, makefile: bool) -> list[str]:
    """Return the targets that the commands in ``text`` pass to ``make`` or ``$(MAKE)``.

    The commands are a Makefile's recipe lines, which start with a tab, or every line of a
    workflow that is not a comment. Variables (``A=b``) and flags are not targets.
    """
    calls: list[str] = []
    for line in text.splitlines():
        if makefile and not line.startswith("\t"):
            continue
        command = line.strip()
        if command.startswith("#"):
            continue
        for part in re.split(r"&&|\|\||[;|]", command):
            words = part.partition(" #")[0].split()
            # A workflow step's first line: - run: make lint
            while words and words[0] in ("-", "run:"):
                words = words[1:]
            if words and words[0].lstrip("@+-").strip("\"'") in ("make", "$(MAKE)"):
                names = (word.strip("\"'") for word in words[1:])
                calls += [n for n in names if n and "=" not in n and not n.startswith("-")]
    return calls


def _dangling_make_calls(root: Path, deletions: list[Path], texts: dict[Path, str]) -> list[str]:
    """Return a line for each make call, after the removal, to a target that it deletes.

    Only the targets that the Makefile defines now and not after the removal count. Any
    other word after ``make`` is left alone: a folder after ``-C``, a variable, a workflow
    expression or a redirect is no target that the removal could break.
    """

    def after(path: Path) -> str | None:
        if path in deletions or not path.is_file():
            return None
        return texts[path] if path in texts else path.read_text(encoding="utf-8")

    makefile = root / "Makefile"
    before = makefile.read_text(encoding="utf-8") if makefile.is_file() else ""
    removed = make_targets(before) - make_targets(after(makefile) or "")
    workflows = sorted((root / ".github" / "workflows").glob("*.y*ml"))
    sources = [(makefile, True), *((workflow, False) for workflow in workflows)]
    problems = []
    for path, is_makefile in sources:
        text = after(path)
        for target in dict.fromkeys(make_calls(text or "", makefile=is_makefile)):
            if target in removed:
                problems.append(
                    f"{path.relative_to(root).as_posix()}: make {target} would call a target "
                    "that the removal deletes"
                )
    return problems


def plan(root: Path) -> tuple[list[Path], dict[Path, str]]:
    """Return the paths to delete, and the new text of each file to change.

    It reads files only. Raises RemovalError for a file that cannot be changed: on a
    first run, a file that has drifted from the text this script expects; on any run, a
    make call to a target that the removal deletes.
    """
    first_run = (root / DELETE[0]).is_dir()
    worker_gone = not (root / WORKER_DIR).is_dir()
    deletions = [root / name for name in DELETE if (root / name).exists()]
    texts: dict[Path, str] = {}

    def current(path: Path) -> str:
        return texts[path] if path in texts else path.read_text(encoding="utf-8")

    config = root / "langgraph.json"
    texts[config] = _without_example_graph(config.read_text(encoding="utf-8"))
    for name in MARKED_FILES:
        path = root / name
        if not path.is_file():
            if first_run:
                raise RemovalError(f"{name} is missing")
            continue
        if first_run and BEGIN not in current(path):
            raise RemovalError(f"{name} has no {BEGIN!r} line: it has drifted")
        try:
            texts[path] = remove_marked_parts(current(path))
        except RemovalError as error:
            raise RemovalError(f"{name}: {error}") from None
    for edit in EDITS:
        path = root / edit.path
        if not path.is_file():
            if first_run and not (worker_gone and edit.path.startswith(f"{WORKER_DIR}/")):
                raise RemovalError(f"{edit.path} is missing")
            continue
        if first_run and edit.old not in current(path):
            first_line = edit.old.strip().splitlines()[0]
            raise RemovalError(
                f"{edit.path} no longer holds the text that this script edits, "
                f"starting {first_line!r}: the file has drifted"
            )
        texts[path] = current(path).replace(edit.old, edit.new)
    if problems := _dangling_make_calls(root, deletions, texts):
        raise RemovalError("; ".join(problems))
    changed = {
        path: text for path, text in texts.items() if text != path.read_text(encoding="utf-8")
    }
    return deletions, changed


def is_repo_root(path: Path) -> bool:
    """Whether ``path`` is the root of this repository: it holds the project's files."""
    return (
        (path / "pyproject.toml").is_file()
        and (path / "langgraph.json").is_file()
        and (path / PACKAGE_DIR).is_dir()
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Remove the example agent from the repository in the current folder."""
    parser = argparse.ArgumentParser(
        prog=PROG,
        description=f"Remove the example agent ({AGENT}) from this repository. "
        "Run it from the repository root.",
    )
    parser.parse_args(argv)
    root = Path.cwd()
    if not is_repo_root(root):
        print(
            f"{PROG}: run this from the repository root, the folder that holds "
            f"pyproject.toml, langgraph.json and {PACKAGE_DIR}/ (not {root})",
            file=sys.stderr,
        )
        return 2
    try:
        deletions, changed = plan(root)
    except RemovalError as error:
        print(f"{PROG}: {error}; nothing was changed", file=sys.stderr)
        return 1
    if not deletions and not changed:
        print("Nothing to change: the example agent is already removed.")
        return 0
    for path in deletions:
        if path.is_dir():
            shutil.rmtree(path)
            print(f"deleted {path.relative_to(root)}/")
        else:
            path.unlink()
            print(f"deleted {path.relative_to(root)}")
    for path, text in changed.items():
        path.write_text(text, encoding="utf-8")
        print(f"edited  {path.relative_to(root)}")
    print(
        f"\nRemoved the example agent: {len(deletions)} deleted, {len(changed)} edited. Next:\n"
        "- remove the example's dependencies from pyproject.toml and uv.lock:\n"
        f"  uv remove {' '.join(EXAMPLE_DEPENDENCIES)}\n"
        "- update README.md, AGENTS.md and docs/, which still describe the example;\n"
        "- add your agent (docs/guides/customize.md), then run make lint typecheck test."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
