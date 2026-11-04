"""The code bundle: order, overlap removal, the budget and redaction (spec §9.6 item 6, P17)."""

from langgraph_postgres_starter.agents.incident_triage.codeintel.bundle import build_bundle
from langgraph_postgres_starter.agents.incident_triage.codeintel.extract import ExtractedDef
from langgraph_postgres_starter.agents.incident_triage.evidence import TRUNCATED
from langgraph_postgres_starter.agents.incident_triage.schemas import CodeSnippet
from langgraph_postgres_starter.redaction import REDACTED


def _def(
    path: str, start: int, end: int, *, symbol: str | None = None, text: str = ""
) -> ExtractedDef:
    """A definition of lines ``start`` to ``end``; its text says which line is which."""
    text = text or "\n".join(f"{path} line {n}" for n in range(start, end + 1))
    return ExtractedDef(
        path=path, start_line=start, end_line=end, symbol=symbol, language="python", text=text
    )


def _spans(snippets: list[CodeSnippet]) -> list[tuple[str, int, int]]:
    return [(s.path, s.start_line, s.end_line) for s in snippets]


TOP = _def("orders/service.py", 23, 25, symbol="_flatten_customer")
CALLER = _def("orders/service.py", 32, 34, symbol="build_invoice")
# A raise site inside TOP: smaller, so it goes.
INSIDE_TOP = _def("orders/service.py", 24, 24)
RAISE = _def("orders/db.py", 1, 3, text='DSN = "postgres://app:hunter2@db/orders"\npass\npass')
# Another app frame around CALLER: larger, so it stays, in CALLER's place.
AROUND_CALLER = _def("orders/service.py", 30, 40)
OTHER = _def("orders/worker.py", 14, 30, symbol="main")
# The same definition as OTHER, from another frame.
OTHER_AGAIN = _def("orders/worker.py", 14, 30, symbol="main")


def _bundle(budget_chars: int) -> list[CodeSnippet]:
    return build_bundle(
        primary=[TOP, CALLER],
        raise_sites=[INSIDE_TOP, RAISE],
        others=[AROUND_CALLER, OTHER, OTHER_AGAIN],
        budget_chars=budget_chars,
        repo="https://github.com/example/orders",
        commit="0123abc",
    )


def test_bundle_order_dedupe_budget():
    snippets = _bundle(60_000)

    # Primary first, then raise sites, then others; a larger overlapping snippet takes
    # the place of the first snippet it overlaps.
    assert _spans(snippets) == [
        ("orders/service.py", 23, 25),
        ("orders/service.py", 30, 40),
        ("orders/db.py", 1, 3),
        ("orders/worker.py", 14, 30),
    ]
    assert [s.id for s in snippets] == [f"code:snippet:{n}" for n in range(1, 5)]
    first = snippets[0]
    assert (first.repo, first.commit, first.symbol, first.language) == (
        "https://github.com/example/orders",
        "0123abc",
        "_flatten_customer",
        "python",
    )
    assert first.text == TOP.text
    assert "hunter2" not in snippets[2].text
    assert REDACTED in snippets[2].text

    # The budget counts text characters.
    sizes = [len(s.text) for s in snippets]
    assert _spans(_bundle(sum(sizes))) == _spans(snippets)
    assert _spans(_bundle(sum(sizes) - 1)) == _spans(snippets[:3])
    # The bundle stops at the first snippet that does not fit (the second), though the
    # third would fit.
    assert sizes[2] < sizes[1]
    assert _spans(_bundle(sizes[0] + sizes[2])) == _spans(snippets[:1])

    # One snippet always stays: cut to the budget, and ending on the last line it keeps
    # (here a part of line 24).
    (only,) = _bundle(40)
    assert len(only.text) <= 40
    assert only.text == "orders/service.py line 23\nor" + TRUNCATED
    assert (only.start_line, only.end_line) == (23, 24)


def test_bundle_larger_snippet_replaces_all_it_overlaps():
    first, second, around_both = _def("x.py", 1, 3), _def("x.py", 5, 7), _def("x.py", 2, 10)
    elsewhere = _def("y.py", 1, 1)
    snippets = build_bundle(
        primary=[first, elsewhere, second],
        raise_sites=[around_both],
        others=[],
        budget_chars=60_000,
        repo="r",
        commit="c",
    )
    assert _spans(snippets) == [("x.py", 2, 10), ("y.py", 1, 1)]


def test_bundle_empty():
    assert (
        build_bundle(primary=[], raise_sites=[], others=[], budget_chars=10, repo="r", commit="c")
        == []
    )
