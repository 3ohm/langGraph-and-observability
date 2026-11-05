"""The docs: one note per practice, the README and the guides (spec §11, §13.7).

A practice note has four sections, Problem, What the template does, Where and Proof, in
that order, in at most 40 lines. Its Where and Proof sections name files in backticks;
a Proof entry may name one test as ``path::test_name``.

A fork that deletes the example agent (``scripts/remove_example.py``) keeps the notes,
so a Proof path inside an agent folder that no longer exists is not checked (ruling
R29). The README's quickstart runs the example agent, so its check is skipped then too.
A fork that deletes the worker keeps them too: once ``worker/`` is gone, the paths that
``docs/guides/customize.md`` says to delete with it are not checked (spec §13.9).
"""

import re
from pathlib import Path

import pytest
import yaml

import langgraph_and_observability

ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = Path(langgraph_and_observability.__file__).parent
EXAMPLE_AGENT_DIR = PACKAGE_DIR / "agents" / "incident_triage"
PRACTICES = ROOT / "docs" / "practices"
GUIDES = ROOT / "docs" / "guides"
README = ROOT / "README.md"

PRACTICE_IDS = [f"P{n}" for n in range(1, 25)]
SECTIONS = ["Problem", "What the template does", "Where", "Proof"]
MAX_LINES = 40

needs_example_agent = pytest.mark.skipif(
    not EXAMPLE_AGENT_DIR.is_dir(), reason="the example agent was removed"
)

CUSTOMIZE = GUIDES / "customize.md"
WORKER_DIR = PACKAGE_DIR / "worker"
WORKER_SECTION = "Run without the queue worker"
WORKER_LIST = "Delete these files and folders:"
needs_worker = pytest.mark.skipif(not WORKER_DIR.is_dir(), reason="the worker was removed")


def _sections(text: str) -> dict[str, str]:
    """Return the body of each ``## `` section, by heading, in the order of the file."""
    sections: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in text.splitlines():
        if line.startswith("## "):
            current = sections.setdefault(line.removeprefix("## ").strip(), [])
        elif current is not None:
            current.append(line)
    return {heading: "\n".join(body) for heading, body in sections.items()}


def _paths(body: str) -> list[str]:
    """Return the paths that a section names: the backticked tokens that hold a ``/``."""
    return [token for token in re.findall(r"`([^`\s]+)`", body) if "/" in token]


def _in_removed_agent(path: str) -> bool:
    """Whether ``path`` lies in ``src/<package>/agents/<agent>/`` and that folder is gone."""
    parts = Path(path).parts
    if len(parts) >= 4 and parts[0] == "src" and parts[2] == "agents":
        return not ROOT.joinpath(*parts[:4]).is_dir()
    return False


def _worker_removal_paths() -> list[str]:
    """The files and folders, from the repository root, that go with the worker.

    They are the bullets of the list after ``WORKER_LIST`` in customize.md's
    ``WORKER_SECTION``, one backticked path each; a folder ends with ``/``.
    """
    section = _sections(CUSTOMIZE.read_text())[WORKER_SECTION]
    _, found, rest = section.partition(WORKER_LIST)
    assert found, f"customize.md: {WORKER_SECTION!r} has no {WORKER_LIST!r} list"
    paths = []
    for line in rest.strip().splitlines():
        bullet = re.fullmatch(r"- `([^`]+)`", line.strip())
        if bullet is None:
            break
        paths.append(bullet[1].replace("<package>", PACKAGE_DIR.name))
    return paths


def _in_removed_worker(path: str) -> bool:
    """Whether ``path`` went with the worker (customize.md), and the worker is gone."""
    if WORKER_DIR.is_dir():
        return False
    return any(
        path == removed or (removed.endswith("/") and path.startswith(removed))
        for removed in _worker_removal_paths()
    )


def _check_path(note: str, entry: str) -> None:
    """Assert that ``entry`` (``path`` or ``path::test_name``) exists."""
    path, _, test = entry.partition("::")
    target = ROOT / path
    assert target.exists(), f"{note}: {path} does not exist"
    if test:
        pattern = rf"^(async )?def {re.escape(test)}\("
        assert re.search(pattern, target.read_text(), re.MULTILINE), (
            f"{note}: {path} has no test {test}"
        )


@pytest.mark.parametrize("pid", PRACTICE_IDS)
def test_every_practice_has_a_note(pid):
    note = PRACTICES / f"{pid}.md"
    assert note.is_file(), f"{note} is missing"
    proofs = _paths(_sections(note.read_text())["Proof"])
    assert proofs, f"{pid}: the Proof section names no test"
    for entry in proofs:
        if not _in_removed_agent(entry) and not _in_removed_worker(entry.partition("::")[0]):
            _check_path(pid, entry)


@pytest.mark.parametrize("pid", PRACTICE_IDS)
def test_practice_note_format(pid):
    text = (PRACTICES / f"{pid}.md").read_text()
    lines = text.splitlines()
    assert lines[0].startswith(f"# {pid}: "), lines[0]
    assert list(_sections(text)) == SECTIONS
    assert len(lines) <= MAX_LINES, f"{pid} has {len(lines)} lines"


@needs_example_agent
@pytest.mark.parametrize("pid", PRACTICE_IDS)
def test_practice_note_where_paths_exist(pid):
    # The removal also deletes files outside the agent folder (evals.yml, for one), so
    # this check runs only while the example agent is there.
    where = _paths(_sections((PRACTICES / f"{pid}.md").read_text())["Where"])
    assert where, f"{pid}: the Where section names no file"
    for entry in where:
        if not _in_removed_worker(entry):
            _check_path(pid, entry)


@needs_worker
def test_customize_guide_lists_what_goes_with_the_worker():
    # Spec §13.9: the guide's list is what a fork deletes with worker/. It names the
    # worker and every test module that imports it, and each path exists.
    removed = _worker_removal_paths()
    assert f"src/{PACKAGE_DIR.name}/worker/" in removed
    for path in removed:
        assert (ROOT / path).exists(), f"customize.md lists {path}, which does not exist"
    imports_worker = re.compile(rf"^\s*(?:from|import) {PACKAGE_DIR.name}\.worker\b", re.M)
    test_roots = [ROOT / "tests", PACKAGE_DIR / "agents", ROOT / "conftest.py"]
    importers = sorted(
        path.relative_to(ROOT).as_posix()
        for root in test_roots
        for path in ([root] if root.is_file() else root.rglob("*.py"))
        if imports_worker.search(path.read_text(encoding="utf-8"))
    )
    unlisted = [
        path
        for path in importers
        if not any(path == r or (r.endswith("/") and path.startswith(r)) for r in removed)
    ]
    assert importers and unlisted == [], f"customize.md does not list {unlisted}"


def _readme_links() -> list[str]:
    """Return the targets of the README's Markdown links that point into the repository."""
    targets = re.findall(r"\]\(([^)\s]+)\)", README.read_text())
    return [t for t in targets if not re.match(r"[a-z]+:|#", t)]


def test_readme_links_resolve():
    links = _readme_links()
    assert links
    for link in links:
        path = link.partition("#")[0]
        if not _in_removed_agent(path):
            assert (ROOT / path).exists(), f"README.md links {link}, which does not exist"


def test_readme_lists_every_practice_note():
    links = set(_readme_links())
    assert {f"docs/practices/{pid}.md" for pid in PRACTICE_IDS} <= links


def _quickstart_commands() -> list[str]:
    """Return the commands of the first ``bash`` block of the README's Quickstart."""
    quickstart = _sections(README.read_text())["Quickstart"]
    block = re.search(r"```bash\n(.*?)```", quickstart, re.DOTALL)
    assert block, "the Quickstart section has no bash block"
    lines = (line.partition("#")[0].strip() for line in block.group(1).splitlines())
    return [line for line in lines if line]


@needs_example_agent
def test_readme_quickstart_targets_exist():
    makefile = (ROOT / "Makefile").read_text()
    targets = set(re.findall(r"^([a-z0-9-]+):", makefile, re.MULTILINE))
    services = set(yaml.safe_load((ROOT / "compose.yaml").read_text())["services"])
    commands = _quickstart_commands()
    # Spec §13.7: a three-command quickstart.
    assert len(commands) == 3, commands
    for command in commands:
        words = command.split()
        if words[0] == "make":
            for word in words[1:]:
                name, equals, value = word.partition("=")
                if not equals:
                    assert name in targets, f"no make target {name!r}: {command}"
                elif name == "LANG":
                    # make incident LANG=<language>: the Makefile's INCIDENTS lists it.
                    assert f" {value}:" in makefile or f"\t{value}:" in makefile, command
        else:
            assert words[:3] == ["docker", "compose", "logs"], command
            named = [w for w in words[3:] if not w.startswith("-")]
            assert named and set(named) <= services, command


def test_guides_exist():
    for guide in ("quickstart", "run-on-eks", "customize", "upgrade"):
        assert (GUIDES / f"{guide}.md").is_file(), guide
    # Spec §6.1: python -m asyncio ps <pid> for debugging stuck runs.
    assert "python -m asyncio ps" in (GUIDES / "run-on-eks.md").read_text()
