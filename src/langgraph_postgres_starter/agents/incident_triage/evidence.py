"""Helpers that build evidence items: IDs, size caps, log decoding (spec §8.5, §9.2, §9.10).

Every evidence item goes through ``make_item``, which redacts its title, text and meta
values (``redaction.redact``) and caps its text at ``MAX_EVIDENCE_TEXT`` characters.
Redaction runs before the cap, so a cut never splits a secret into a part that the
rules no longer recognise, and the cap holds for the final text.

``cap_text`` cuts in one of two ways. Ordinary text, such as an event message, keeps
its start and ends with ``TRUNCATED``. A log keeps its end (``keep="end"``), because a
crash is at the end of a log: the text starts with ``TRUNCATED`` on a line of its own,
then the last whole lines that fit.
"""

import re
from collections.abc import Mapping
from typing import Literal

from langgraph_postgres_starter.agents.incident_triage.schemas import (
    MAX_EVIDENCE_TEXT,
    EvidenceItem,
)
from langgraph_postgres_starter.redaction import redact

# The marker that ends a cut line or text, or starts a log that lost its first lines.
TRUNCATED = "…[truncated]"

# The values of EvidenceItem.kind.
EvidenceKind = Literal["k8s_status", "k8s_event", "log", "node", "datadog_event", "note"]

# The characters that the key of an evidence ID may hold (schemas.EVIDENCE_ID_PATTERN).
_UNSAFE_KEY_CHARS = re.compile(r"[^A-Za-z0-9._/-]")


def make_id(kind: str, sub: str, key: str) -> str:
    """The evidence ID ``<kind>:<sub>:<key>``, such as ``k8s:event:3``.

    Characters that an ID's key may not hold become ``_``, and an empty key is ``_``.
    """
    return f"{kind}:{sub}:{_UNSAFE_KEY_CHARS.sub('_', key) or '_'}"


def cap_text(
    text: str, max_chars: int = MAX_EVIDENCE_TEXT, *, keep: Literal["start", "end"] = "start"
) -> str:
    """Return ``text`` cut to at most ``max_chars`` characters.

    Text that fits comes back unchanged. With ``keep="start"`` the result is the start
    of ``text`` followed by ``TRUNCATED``. With ``keep="end"`` (for logs) it is
    ``TRUNCATED``, a newline, and the end of ``text`` from the first line that fits
    whole; a single line longer than the room keeps its last characters.
    """
    if len(text) <= max_chars:
        return text
    if keep == "start":
        room = max_chars - len(TRUNCATED)
        return text[:room] + TRUNCATED if room > 0 else text[:max_chars]
    room = max_chars - len(TRUNCATED) - 1
    if room <= 0:
        return text[len(text) - max_chars :]
    kept = text[len(text) - room :]
    newline = kept.find("\n")
    if 0 <= newline < len(kept) - 1:
        kept = kept[newline + 1 :]
    return f"{TRUNCATED}\n{kept}"


def decode_log(raw: bytes | str, *, max_line: int = 2000) -> str:
    """Return a log as text: UTF-8 with replacement characters, lines cut at ``max_line``.

    A line longer than ``max_line`` characters keeps its first ``max_line`` characters
    and ends with ``TRUNCATED``. Lines are split at ``\\n`` only.
    """
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
    return "\n".join(
        line if len(line) <= max_line else line[:max_line] + TRUNCATED for line in text.split("\n")
    )


def number_lines(text: str, start: int = 1) -> str:
    """Prefix each line of ``text`` with its number and ``": "``, counting from ``start``.

    A final newline does not start another line, and the result has none.
    """
    lines = text.split("\n")
    if lines[-1] == "":
        lines.pop()
    return "\n".join(f"{number}: {line}" for number, line in enumerate(lines, start))


def make_item(
    item_id: str,
    kind: EvidenceKind,
    title: str,
    text: str,
    meta: Mapping[str, str] | None = None,
    *,
    keep: Literal["start", "end"] = "start",
) -> EvidenceItem:
    """An evidence item, redacted, with its text capped (``cap_text`` with ``keep``)."""
    return EvidenceItem(
        id=item_id,
        kind=kind,
        title=redact(title),
        text=cap_text(redact(text), keep=keep),
        meta={key: redact(value) for key, value in (meta or {}).items()},
    )
