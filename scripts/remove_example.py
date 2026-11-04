"""Remove the example agent, Incident Triage, from a fork (spec §13.9; P22).

Run it from the repository root::

    uv run scripts/remove_example.py

It deletes the agent's package, ``src/<package>/agents/incident_triage/``, and the files
that only the example uses: ``scripts/send_incident.py`` (``make incident``) and
``scripts/sync_dataset.py``, their tests, the agent's end-to-end test and the Evals
workflow. It removes the agent's entry from ``langgraph.json`` and its line from
``worker/registry.py``, and it resets the two settings defaults that name it
(``intake_agent`` to ``""``, ``fixtures_dir`` to the package's ``fixtures`` folder) in
``settings.py``, in their test and in ``.env.example``. It also removes the example's
parts of the build: the ``grammars`` and ``incident`` make targets, the Dockerfile's
tree-sitter grammars and the CI steps that fetch them. A file marks such a part with a
``# example-agent:begin`` line and a ``# example-agent:end`` line; the part goes with
the lines between them, and the blank lines after it.

The script works out every change before it makes one, then prints each change as it
makes it. It changes only what still holds the example's text, so a second run changes
nothing. It refuses to run outside the repository root, imports nothing from the
package and uses the standard library only.

Afterwards the docs (``README.md``, ``AGENTS.md``, ``docs/``) still describe the
example: update them for your fork. ``docs/guides/customize.md`` shows how to add your
own agent.
"""

import argparse
import json
import shutil
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

PROG = "remove_example.py"
PACKAGE = "langgraph_postgres_starter"
AGENT = "incident_triage"
PACKAGE_DIR = f"src/{PACKAGE}"

# The files and folders that only the example agent uses.
DELETE = (
    f"{PACKAGE_DIR}/agents/{AGENT}",
    "scripts/send_incident.py",
    "scripts/sync_dataset.py",
    "tests/unit/test_send_incident.py",
    "tests/unit/test_sync_dataset.py",
    "tests/e2e/test_triage_e2e.py",
    ".github/workflows/evals.yml",
)

# The lines that open and close a part of a file that only the example agent uses.
BEGIN = "# example-agent:begin"
END = "# example-agent:end"
MARKED_FILES = ("Makefile", "Dockerfile", "tests/unit/test_ci_config.py")


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
    # The settings defaults that name the agent (ruling R29), with their test and docs.
    Edit(f"{PACKAGE_DIR}/settings.py", f'intake_agent: str = "{AGENT}"', 'intake_agent: str = ""'),
    Edit(
        f"{PACKAGE_DIR}/settings.py",
        f'_PACKAGE_DIR / "agents" / "{AGENT}" / "fixtures"',
        '_PACKAGE_DIR / "fixtures"',
    ),
    Edit("tests/unit/test_settings.py", f'"intake_agent": "{AGENT}"', '"intake_agent": ""'),
    Edit(
        "tests/unit/test_settings.py",
        f'PACKAGE_DIR / "agents" / "{AGENT}" / "fixtures"',
        'PACKAGE_DIR / "fixtures"',
    ),
    Edit(".env.example", f"APP_INTAKE_AGENT={AGENT}\n", "APP_INTAKE_AGENT=\n"),
    Edit(
        ".env.example",
        f"the package's agents/{AGENT}/fixtures directory",
        "the package's fixtures directory",
    ),
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


def plan(root: Path) -> tuple[list[Path], dict[Path, str]]:
    """Return the paths to delete, and the new text of each file to change.

    It reads files only. Raises RemovalError for a file that cannot be changed.
    """
    deletions = [root / name for name in DELETE if (root / name).exists()]
    texts: dict[Path, str] = {}

    def current(path: Path) -> str:
        return texts[path] if path in texts else path.read_text(encoding="utf-8")

    config = root / "langgraph.json"
    texts[config] = _without_example_graph(config.read_text(encoding="utf-8"))
    for name in MARKED_FILES:
        path = root / name
        if path.is_file():
            try:
                texts[path] = remove_marked_parts(current(path))
            except RemovalError as error:
                raise RemovalError(f"{name}: {error}") from None
    for edit in EDITS:
        path = root / edit.path
        if path.is_file():
            texts[path] = current(path).replace(edit.old, edit.new)
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
        "- update README.md, AGENTS.md and docs/, which still describe the example;\n"
        "- add your agent (docs/guides/customize.md), then run make lint typecheck test."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
