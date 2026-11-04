"""Log literals, and the ripgrep search for them in a checkout (spec §9.6 item 5).

A crash often prints text that the code holds as a constant: an exception message, or
the last log message before a native crash that prints no trace. ``literals_from_logs``
takes those constants from the log, and ``search_literals`` finds them with ``rg``.

``literals_from_logs``:

- A candidate line contains ``Exception``, ``Error``, ``panic:``, ``error=`` or
  ``FATAL``, or has ``processing`` after a log level. A log level is a word of
  ``LOG_LEVELS`` in capitals, such as ``INFO`` in ``worker.INFO:``, or ``level=<word>``.
- The line is redacted first (``redaction.redact``). Its text after the log level (the
  whole line when it has none) is split at its variable parts: runs of 2 or more digits
  and numbers that stand alone (a digit inside a word, as in ``utf8``, stays), ``0x``
  hex, UUIDs, IPv4 and IPv6 addresses, quoted strings, the redaction placeholder (so a
  literal never holds a redacted value), and C0 control characters other than tab, as
  in binary junk (so a literal never holds a NUL byte, which no process argument can
  hold, and a constant next to the junk is still found).
- Each piece loses the whitespace and punctuation at its ends. The line's literal is
  its longest piece of at least ``MIN_LITERAL_CHARS`` characters, the first of equal
  ones; a piece that redaction would change is not used.
- The literals are newest first (a crash is at the end of a log), without duplicates,
  at most ``max_literals``.

``search_literals`` runs ``rg --json -F --max-count 5 --no-config --sort path -e <literal>
... -- <root>`` (no shell; each literal follows ``-e``, so one that starts with ``-`` is
still a pattern). ``rg`` searches as it does by default: it skips hidden and ignored
files and does not follow symbolic links. ``--sort path`` makes the order of the matches,
and so the cap, the same on every run. The search reads only ``match`` events, at most
``MAX_OUTPUT_BYTES`` of output, skips an event longer than ``MAX_EVENT_BYTES``, and
stops at ``max_matches``. After ``SEARCH_TIMEOUT_S`` seconds it stops ``rg`` and returns
the matches found until then. ``rg``'s errors, such as a file it cannot read, do not
fail the search.
"""

import asyncio
import contextlib
import json
import re
import shutil
import string
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from langgraph_postgres_starter.logging import get_logger
from langgraph_postgres_starter.redaction import REDACTED, redact

_log = get_logger(__name__)

# The shortest literal: a shorter one matches too much code.
MIN_LITERAL_CHARS = 12
# The most matching lines that rg reports per file (--max-count).
MAX_MATCHES_PER_FILE = 5
SEARCH_TIMEOUT_S = 30.0
# The most output read from rg, and the longest event (one JSON line) parsed.
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
MAX_EVENT_BYTES = 256 * 1024
_CHUNK_BYTES = 64 * 1024

LOG_LEVELS = ("TRACE", "DEBUG", "INFO", "NOTICE", "WARN", "WARNING", "ERROR", "CRITICAL", "FATAL")
_LEVEL = re.compile(rf"\b(?:{'|'.join(LOG_LEVELS)})\b|\blevel=\w+")
# A line with one of these is a candidate; with _AFTER_LEVEL, only after a log level.
_MARKERS = ("Exception", "Error", "panic:", "error=", "FATAL")
_AFTER_LEVEL = "processing"

_HEX = "[0-9A-Fa-f]"
# The variable parts of a line, the first alternative first where several match.
_VARIABLE = re.compile(
    "|".join(
        (
            # C0 control characters except tab.
            r"[\x00-\x08\x0a-\x1f]+",
            re.escape(REDACTED),
            r'"[^"\n]*"',
            # Not after a letter, so the apostrophe in "can't" opens no quote.
            r"(?<!\w)'[^'\n]*'",
            rf"\b{_HEX}{{8}}-{_HEX}{{4}}-{_HEX}{{4}}-{_HEX}{{4}}-{_HEX}{{12}}\b",
            rf"\b0[xX]{_HEX}+",
            # IPv6: at least three groups of hex digits with colons, and a digit.
            rf"(?<![\w:.])(?=[0-9A-Fa-f:]*\d){_HEX}{{0,4}}(?::{_HEX}{{0,4}}){{2,7}}(?![\w:])",
            r"(?<![\w.])\d{1,3}(?:\.\d{1,3}){3}(?![\w.])",
            r"\d{2,}",
            r"(?<!\w)\d(?!\w)",
        )
    )
)
_TRIM = string.whitespace + string.punctuation
# The characters that search_literals never passes to rg in a literal.
_UNSEARCHABLE = frozenset("\n\x00")


class RipgrepMissing(RuntimeError):
    """``rg`` is not on ``PATH``."""

    def __init__(self) -> None:
        super().__init__(
            "ripgrep (rg) is not on PATH, and code evidence searches the source with it: "
            "install ripgrep (the image installs Debian's ripgrep package)"
        )


@dataclass(frozen=True)
class SearchMatch:
    """Line ``line`` (from 1) of the file ``path`` (relative to the root) holds ``literal``."""

    path: str
    line: int
    literal: str


def literals_from_logs(log_text: str, *, max_literals: int = 10) -> list[str]:
    """The constant text of the log's error and log lines, newest first (module docstring)."""
    literals: list[str] = []
    for line in reversed(log_text.splitlines()):
        if len(literals) >= max_literals:
            break
        literal = _line_literal(line)
        if literal is not None and literal not in literals:
            literals.append(literal)
    return literals


def _line_literal(line: str) -> str | None:
    """The line's literal, or None when it is no candidate or has no piece long enough."""
    line = redact(line)
    level = _LEVEL.search(line)
    message = line if level is None else line[level.end() :]
    if not any(marker in line for marker in _MARKERS) and (
        level is None or _AFTER_LEVEL not in message
    ):
        return None
    pieces = (piece.strip(_TRIM) for piece in _VARIABLE.split(message))
    usable = [p for p in pieces if len(p) >= MIN_LITERAL_CHARS and redact(p) == p]
    return max(usable, key=len, default=None)


async def search_literals(
    root: Path, literals: Sequence[str], *, max_matches: int = 50
) -> list[SearchMatch]:
    """The lines below ``root`` that hold one of ``literals``, at most ``max_matches``.

    Each match names the literal it holds; a line with two literals is two matches. The
    matches are in path order, then line order. An empty literal is not searched, nor is
    one with a line break (``rg`` matches within a line) or a NUL byte (no process
    argument can hold one). Raises RipgrepMissing when ``rg`` is not on ``PATH``; with no
    literal, it returns ``[]`` and does not run ``rg``.
    """
    wanted = list(dict.fromkeys(lit for lit in literals if lit and not _UNSEARCHABLE & set(lit)))
    if not wanted or max_matches < 1:
        return []
    rg = shutil.which("rg")
    if rg is None:
        raise RipgrepMissing
    args = [rg, "--json", "-F", "--max-count", str(MAX_MATCHES_PER_FILE), "--no-config"]
    args += ["--sort", "path"]
    for literal in wanted:
        args += ["-e", literal]
    args += ["--", str(root)]
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError as error:
        # rg was removed between the lookup and the start.
        raise RipgrepMissing from error
    matches: list[SearchMatch] = []
    try:
        if process.stdout is None:
            raise RuntimeError("rg was started without a stdout pipe")
        async with asyncio.timeout(SEARCH_TIMEOUT_S):
            await _read_matches(process.stdout, root, set(wanted), max_matches, matches)
    except TimeoutError:
        _log.warning("code.search.timeout", timeout_s=SEARCH_TIMEOUT_S, matches=len(matches))
    finally:
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
        await process.wait()
    return matches


async def _read_matches(
    stream: asyncio.StreamReader,
    root: Path,
    wanted: set[str],
    max_matches: int,
    matches: list[SearchMatch],
) -> None:
    """Add the matches of rg's output to ``matches``, until ``max_matches`` or its end.

    It reads at most ``MAX_OUTPUT_BYTES``, and skips an event longer than
    ``MAX_EVENT_BYTES``. ``matches`` keeps what it found if the read is cancelled.
    """
    pending = b""
    # True while reading the rest of an event that was too long.
    skipping = False
    total = 0
    while total < MAX_OUTPUT_BYTES:
        chunk = await stream.read(min(_CHUNK_BYTES, MAX_OUTPUT_BYTES - total))
        if not chunk:
            return
        total += len(chunk)
        *events, pending = (pending + chunk).split(b"\n")
        for event in events:
            if skipping:
                skipping = False
                continue
            for match in _event_matches(event, root, wanted):
                if match not in matches:
                    matches.append(match)
                    if len(matches) == max_matches:
                        return
        if len(pending) > MAX_EVENT_BYTES:
            pending, skipping = b"", True


def _event_matches(event: bytes, root: Path, wanted: set[str]) -> list[SearchMatch]:
    """The matches in one line of rg's JSON output; none for an event that is not a match.

    A path that is not UTF-8 (rg sends it as ``bytes``) is skipped.
    """
    try:
        message: Any = json.loads(event)
    except ValueError:
        return []
    if not isinstance(message, dict) or message.get("type") != "match":
        return []
    data = message.get("data") or {}
    path = (data.get("path") or {}).get("text")
    line = data.get("line_number")
    if not isinstance(path, str) or not isinstance(line, int):
        return []
    try:
        relative = Path(path).relative_to(root).as_posix()
    except ValueError:
        return []
    found: list[SearchMatch] = []
    for submatch in data.get("submatches") or []:
        text = (submatch.get("match") or {}).get("text")
        if isinstance(text, str) and text in wanted:
            found.append(SearchMatch(path=relative, line=line, literal=text))
    return found
