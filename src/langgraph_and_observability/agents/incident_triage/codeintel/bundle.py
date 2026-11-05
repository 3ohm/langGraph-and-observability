"""The code bundle: the snippets that the analysis sees (spec §9.6 item 6, §8.5).

``build_bundle`` ranks the extracted definitions: the top app frames (``primary``) first,
then the places that raise or log the error (``raise_sites``), then the other app
frames (``others``), each group in the order given.

- Overlap: when two definitions of the same path share a line, the larger one (more
  lines, then more characters; the first on a tie) stays, in the place of the first of
  the two. So the top app frame's code always comes first.
- Text: a snippet's text is its definition's text, redacted (``redaction.redact``).
- Budget: snippets are kept in order while their texts fit in ``budget_chars``
  characters in all, and the bundle stops at the first one that does not fit. The first
  snippet always stays: when it alone is longer than the budget, its text is cut to the
  budget (``evidence.cap_text``) and its ``end_line`` is the line that the cut ends on.
- IDs: ``code:snippet:<n>``, from 1, in the bundle's order.
"""

from collections.abc import Sequence

from langgraph_and_observability.agents.incident_triage.codeintel.extract import ExtractedDef
from langgraph_and_observability.agents.incident_triage.evidence import cap_text, make_id
from langgraph_and_observability.agents.incident_triage.schemas import CodeSnippet
from langgraph_and_observability.redaction import redact


def build_bundle(
    *,
    primary: Sequence[ExtractedDef],
    raise_sites: Sequence[ExtractedDef],
    others: Sequence[ExtractedDef],
    budget_chars: int,
    repo: str,
    commit: str,
) -> list[CodeSnippet]:
    """The snippets of ``repo`` at ``commit``, ranked, without overlaps, within the budget."""
    snippets: list[CodeSnippet] = []
    used = 0
    for definition in _without_overlaps([*primary, *raise_sites, *others]):
        text = redact(definition.text)
        end_line = definition.end_line
        if used + len(text) > budget_chars:
            if snippets:
                break
            text = cap_text(text, max(budget_chars, 0))
            end_line = definition.start_line + text.count("\n")
        used += len(text)
        snippets.append(
            CodeSnippet(
                id=make_id("code", "snippet", str(len(snippets) + 1)),
                repo=repo,
                commit=commit,
                path=definition.path,
                start_line=definition.start_line,
                end_line=end_line,
                symbol=definition.symbol,
                language=definition.language,
                text=text,
            )
        )
    return snippets


def _without_overlaps(definitions: Sequence[ExtractedDef]) -> list[ExtractedDef]:
    """``definitions`` in order, each overlapping group reduced to its largest member."""
    kept: list[ExtractedDef] = []
    for candidate in definitions:
        clashes = [index for index, other in enumerate(kept) if _overlap(other, candidate)]
        if not clashes:
            kept.append(candidate)
            continue
        # The kept definitions never overlap each other, so the candidate replaces every
        # one it overlaps, or none.
        if _size(candidate) > max(_size(kept[index]) for index in clashes):
            kept[clashes[0]] = candidate
            for index in reversed(clashes[1:]):
                del kept[index]
    return kept


def _overlap(a: ExtractedDef, b: ExtractedDef) -> bool:
    return a.path == b.path and a.start_line <= b.end_line and b.start_line <= a.end_line


def _size(definition: ExtractedDef) -> tuple[int, int]:
    return definition.end_line - definition.start_line, len(definition.text)
