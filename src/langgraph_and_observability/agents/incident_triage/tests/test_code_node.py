"""The collect_code step on incident fixtures, through the simulated adapters (spec §9.6).

Each test runs the steps before it, ``resolve_target`` and ``collect_k8s``, on a fixture
as the graph does in simulated mode, then ``collect_code``. The python fixture has a
stack trace; the c fixture has none, so its code comes from its log messages. Without
a source, or with frames that are not in the source, the step records a note and
returns (Review Focus 4).
"""

import pytest
from langgraph.runtime import Runtime

from langgraph_and_observability.agent_spec import RunInfo
from langgraph_and_observability.agents.incident_triage.adapters import build_adapters
from langgraph_and_observability.agents.incident_triage.codeintel.extract import (
    enclosing_definition,
)
from langgraph_and_observability.agents.incident_triage.codeintel.languages import (
    GrammarMissingError,
)
from langgraph_and_observability.agents.incident_triage.codeintel.search import (
    SearchMatch,
    literals_from_logs,
    search_literals,
)
from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace import parse_frames
from langgraph_and_observability.agents.incident_triage.context import TriageContext
from langgraph_and_observability.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_and_observability.agents.incident_triage.nodes import code
from langgraph_and_observability.agents.incident_triage.nodes.code import (
    MAX_LISTED_FRAMES,
    collect_code,
)
from langgraph_and_observability.agents.incident_triage.nodes.k8s import collect_k8s
from langgraph_and_observability.agents.incident_triage.nodes.resolve import resolve_target
from langgraph_and_observability.agents.incident_triage.schemas import IncidentMessage
from langgraph_and_observability.agents.incident_triage.state import TriageState
from langgraph_and_observability.ids import new_id
from langgraph_and_observability.settings import load_settings


def _runtime(fixture_id: str) -> Runtime[TriageContext]:
    settings = load_settings()
    adapters = build_adapters(settings, fixture_id=fixture_id)
    run = RunInfo(run_id=new_id(), dedup_key="test", attempt=1)
    return Runtime(
        context=TriageContext(run=run, settings=settings, adapters=adapters, fixture_id=fixture_id)
    )


async def _state_before_code(fixture_id: str, runtime: Runtime[TriageContext]) -> TriageState:
    """The state after ``resolve_target`` and ``collect_k8s`` (notes aside)."""
    message = IncidentMessage.model_validate(load_fixture(fixture_id).incident)
    state: TriageState = {"message": message}
    state.update(await resolve_target(state, runtime))
    state.update(await collect_k8s(state, runtime))
    return state


def _expected_code(fixture_id: str) -> tuple[str, int, int, str]:
    code = load_fixture(fixture_id).expected["code"]
    return code["path"], code["start_line"], code["end_line"], code["symbol"]


async def test_collect_code_python_first_snippet_is_bug():
    runtime = _runtime("python-keyerror")
    state = await _state_before_code("python-keyerror", runtime)

    update = await collect_code(state, runtime)

    snippets = update["code_evidence"]
    first = snippets[0]
    assert first.path == _expected_code("python-keyerror")[0]
    assert (first.path, first.start_line, first.end_line, first.symbol) == _expected_code(
        "python-keyerror"
    )
    assert (first.id, first.repo, first.commit) == ("code:snippet:1", "fixture://python", "fixture")
    # The other app frames follow, innermost first.
    assert [s.path for s in snippets] == [
        "orders/service.py",
        "orders/service.py",
        "orders/worker.py",
    ]
    assert [s.id for s in snippets] == ["code:snippet:1", "code:snippet:2", "code:snippet:3"]
    assert update["notes"] == []


async def test_c_segfault_uses_log_literals():
    runtime = _runtime("c-segfault")
    state = await _state_before_code("c-segfault", runtime)
    assert parse_frames(state["log_text"]) == []

    update = await collect_code(state, runtime)

    snippets = update["code_evidence"]
    assert [(s.path, s.start_line, s.end_line, s.symbol) for s in snippets] == [
        _expected_code("c-segfault")
    ]
    assert "processing order batch for warehouse" in snippets[0].text
    assert (snippets[0].repo, snippets[0].language) == ("fixture://c", "c")
    assert update["notes"] == []


async def test_collect_code_without_source_continues():
    # An infrastructure incident: the fixture has no source.json.
    runtime = _runtime("infra-oomkilled")
    state = await _state_before_code("infra-oomkilled", runtime)

    update = await collect_code(state, runtime)

    assert update["code_evidence"] == []
    (note,) = update["notes"]
    assert (note.id, note.kind) == ("note:code:source_unavailable", "note")


async def test_unmapped_frames_are_recorded():
    runtime = _runtime("python-keyerror")
    state = await _state_before_code("python-keyerror", runtime)
    frame = 'File "/app/orders/worker.py", line 34, in <module>'
    assert frame in state["log_text"]
    state["log_text"] = state["log_text"].replace(
        frame, 'File "/app/orders/legacy_main.py", line 34, in <module>'
    )

    update = await collect_code(state, runtime)

    (note,) = update["notes"]
    assert (note.id, note.kind) == ("note:code:unmapped_frames", "note")
    assert "/app/orders/legacy_main.py:34" in note.text
    assert "service.py" not in note.text
    # The frames that map still give their code, the bug first.
    assert update["code_evidence"][0].path == "orders/service.py"

    # A long list is cut, and says how many frames it leaves out.
    many = MAX_LISTED_FRAMES + 3
    state["log_text"] = "Traceback (most recent call last):\n" + "\n".join(
        f'  File "/app/gone/module_{n}.py", line {n + 1}, in f{n}' for n in range(many)
    )
    update = await collect_code(state, runtime)
    (note,) = update["notes"]
    assert note.text.count("/app/gone/module_") == MAX_LISTED_FRAMES
    assert "and 3 more" in note.text
    assert update["code_evidence"] == []


async def test_nul_bytes_in_the_log_do_not_fail_the_step():
    runtime = _runtime("python-keyerror")
    state = await _state_before_code("python-keyerror", runtime)
    # Binary junk, as a native crash can print.
    state["log_text"] += "\nError: decoder rejected input abc\x00defghijklmnop qrstuv\n\x00\x00\x00"

    update = await collect_code(state, runtime)

    assert [s.path for s in update["code_evidence"]] == [
        "orders/service.py",
        "orders/service.py",
        "orders/worker.py",
    ]
    assert update["notes"] == []


async def test_match_inside_a_chosen_definition_is_not_extracted_again(monkeypatch):
    runtime = _runtime("python-keyerror")
    state = await _state_before_code("python-keyerror", runtime)
    # Its literal is the docstring of build_invoice, which a frame already gives.
    literal = "The invoice of a paid order, from its orders.paid event"
    state["log_text"] += f'\nError: "x" {literal}'
    assert literals_from_logs(state["log_text"]) == [literal]
    app = runtime.context.settings.fixtures_dir / "apps" / "python"
    assert await search_literals(app, [literal]) == [
        SearchMatch(path="orders/service.py", line=33, literal=literal)
    ]
    extracted: list[tuple[str, int]] = []

    def counting(root, path, line, **kwargs):
        extracted.append((path, line))
        return enclosing_definition(root, path, line, **kwargs)

    monkeypatch.setattr(code, "enclosing_definition", counting)

    update = await collect_code(state, runtime)

    assert ("orders/service.py", 33) not in extracted
    assert len(extracted) == 4  # the four app frames
    assert [s.path for s in update["code_evidence"]] == [
        "orders/service.py",
        "orders/service.py",
        "orders/worker.py",
    ]


async def test_unreadable_file_is_skipped_and_setup_errors_fail_the_step(monkeypatch):
    runtime = _runtime("python-keyerror")
    state = await _state_before_code("python-keyerror", runtime)

    def unreadable_worker(root, path, line, **kwargs):
        if path == "orders/worker.py":
            raise PermissionError(f"cannot read {path}")
        return enclosing_definition(root, path, line, **kwargs)

    monkeypatch.setattr(code, "enclosing_definition", unreadable_worker)
    update = await collect_code(state, runtime)
    assert [s.path for s in update["code_evidence"]] == ["orders/service.py", "orders/service.py"]

    def no_grammar(root, path, line, **kwargs):
        raise GrammarMissingError("python", "/no/grammars")

    monkeypatch.setattr(code, "enclosing_definition", no_grammar)
    with pytest.raises(GrammarMissingError):
        await collect_code(state, runtime)
