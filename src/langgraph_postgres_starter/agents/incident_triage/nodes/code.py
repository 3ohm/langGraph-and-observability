"""The ``collect_code`` step: the source code around the error (spec §9.2, §9.6).

1. The locator finds the target's repository and commit, with the annotations of the
   target's workload (none when the target has no workload). Without a source the step
   records ``note:code:source_unavailable`` and returns no code; the run goes on to
   ``analyze`` (Review Focus 4).
2. The source provider checks the commit out, and ``FileIndex`` indexes its files.
3. Frames: the app frames of the stack traces in ``log_text`` (``parse_frames``) that
   map to a file of the checkout, innermost first. The first ``PRIMARY_FRAMES`` are
   primary, the rest are others. The app frames that map to no file are listed in one
   note, ``note:code:unmapped_frames``, as ``<file>:<line>``, at most
   ``MAX_LISTED_FRAMES`` of them (Review Focus 4).
4. Raise sites: ripgrep matches of the log's literals (``literals_from_logs``,
   ``search_literals``). A match inside a definition already chosen is skipped.
5. The definition around each line (``enclosing_definition``) goes into the bundle
   (``build_bundle``), within ``code_budget_chars`` (``APP_CODE_BUDGET_CHARS``). A
   line whose file cannot be read (OSError) or leads out of the checkout (ValueError)
   is skipped. A grammar that was not fetched (``GrammarMissingError``) and a missing
   ``rg`` (``RipgrepMissing``) are setup errors: they fail the step.

Indexing and extraction read files and parse them, so each runs in a thread.
"""

import asyncio
from collections.abc import Sequence
from pathlib import Path

from langgraph.runtime import Runtime

from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import SourceRef
from langgraph_postgres_starter.agents.incident_triage.codeintel.bundle import build_bundle
from langgraph_postgres_starter.agents.incident_triage.codeintel.extract import (
    ExtractedDef,
    enclosing_definition,
)
from langgraph_postgres_starter.agents.incident_triage.codeintel.paths import FileIndex
from langgraph_postgres_starter.agents.incident_triage.codeintel.search import (
    literals_from_logs,
    search_literals,
)
from langgraph_postgres_starter.agents.incident_triage.codeintel.stacktrace import parse_frames
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext, ctx
from langgraph_postgres_starter.agents.incident_triage.evidence import make_id, make_item
from langgraph_postgres_starter.agents.incident_triage.schemas import EvidenceItem, Frame, Target
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.logging import get_logger

_log = get_logger(__name__)

# The app frames whose code comes first in the bundle.
PRIMARY_FRAMES = 3
# The most unmapped frames that the note lists.
MAX_LISTED_FRAMES = 20


async def collect_code(state: TriageState, runtime: Runtime[TriageContext]) -> TriageState:
    """Collect the code evidence. Returns ``{"code_evidence": [...], "notes": [...]}``."""
    context = ctx(runtime)
    adapters = context.adapters
    target = state["target"]
    log_text = state.get("log_text", "")
    frames = [frame for frame in parse_frames(log_text) if frame.is_app_frame]
    annotations = (
        {}
        if target.workload is None
        else await adapters.k8s.get_workload_annotations(target.namespace, target.workload)
    )
    ref = await adapters.locator.locate(target, annotations)
    if ref is None:
        return {"code_evidence": [], "notes": [_source_unavailable(target)]}
    root = await adapters.source.checkout(ref)
    index = await asyncio.to_thread(FileIndex.build, root)

    mapped: list[tuple[str, int]] = []
    unmapped: list[Frame] = []
    for frame in frames:
        path = index.resolve(frame)
        if path is None:
            unmapped.append(frame)
        else:
            mapped.append((path, frame.line))
    primary = await _extract_all(root, mapped[:PRIMARY_FRAMES])
    others = await _extract_all(root, mapped[PRIMARY_FRAMES:])

    chosen = [*primary, *others]
    raise_sites: list[ExtractedDef] = []
    for match in await search_literals(root, literals_from_logs(log_text)):
        if any(_contains(definition, match.path, match.line) for definition in chosen):
            continue
        found = await _extract(root, match.path, match.line)
        if found is not None:
            raise_sites.append(found)
            chosen.append(found)

    code = build_bundle(
        primary=primary,
        raise_sites=raise_sites,
        others=others,
        budget_chars=context.settings.code_budget_chars,
        repo=ref.repo_url,
        commit=ref.commit,
    )
    notes = [_unmapped_frames(unmapped, ref)] if unmapped else []
    return {"code_evidence": code, "notes": notes}


async def _extract_all(root: Path, lines: Sequence[tuple[str, int]]) -> list[ExtractedDef]:
    """The definitions around ``(path, line)`` pairs, in order, without the ones that fail."""
    found = [await _extract(root, path, line) for path, line in lines]
    return [definition for definition in found if definition is not None]


async def _extract(root: Path, path: str, line: int) -> ExtractedDef | None:
    """The definition around ``line`` of ``path``, or None (``enclosing_definition``).

    None also when the file cannot be read or leads out of ``root``.
    """
    try:
        return await asyncio.to_thread(enclosing_definition, root, path, line)
    except (OSError, ValueError) as error:
        _log.warning("code.extract.skipped", path=path, line=line, error=str(error))
        return None


def _contains(definition: ExtractedDef, path: str, line: int) -> bool:
    return definition.path == path and definition.start_line <= line <= definition.end_line


def _source_unavailable(target: Target) -> EvidenceItem:
    what = (
        f"{target.workload.kind} {target.workload.name}"
        if target.workload is not None
        else f"pod {target.pod}"
    )
    return make_item(
        make_id("note", "code", "source_unavailable"),
        "note",
        "No source code found",
        f"No source names the repository and commit of {what} in namespace "
        f"{target.namespace}, so the analysis has no code evidence.",
    )


def _unmapped_frames(frames: Sequence[Frame], ref: SourceRef) -> EvidenceItem:
    places = list(dict.fromkeys(f"{frame.file}:{frame.line}" for frame in frames))
    listed = places[:MAX_LISTED_FRAMES]
    lines = [
        "These app frames of the stack trace name files that are not in the source at "
        f"{ref.repo_url} (commit {ref.commit}), so their code is missing:",
        *listed,
    ]
    if len(places) > len(listed):
        lines.append(f"and {len(places) - len(listed)} more")
    return make_item(
        make_id("note", "code", "unmapped_frames"),
        "note",
        "Stack frames not found in the source",
        "\n".join(lines),
    )
