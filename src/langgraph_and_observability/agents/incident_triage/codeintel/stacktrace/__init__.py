"""Stack frames from logs, one parser per family (spec §9.5, §9.6 item 2).

Each family's module has ``detect(text) -> bool``, a cheap check for a frame of the
family's format, and ``parse(text) -> list[Frame]``: the frames innermost first, each
marked as an app frame or a library frame. ``parse_frames`` runs the parser of each
family that ``detect_families`` finds, in the order of ``_PARSERS``, and keeps the
first of frames that have the same family, file, line and function.

A family is added with its module and an entry in ``_PARSERS``.
"""

from collections.abc import Callable
from typing import NamedTuple

from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace import (
    go,
    jvm,
    native,
    node,
    php,
    python,
    ruby,
)
from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_and_observability.agents.incident_triage.schemas import Frame

__all__ = ["Family", "detect_families", "parse_frames"]


class _Parser(NamedTuple):
    """A family's check for its trace format, and its parser."""

    detect: Callable[[str], bool]
    parse: Callable[[str], list[Frame]]


# The parser of each family, in the order parse_frames runs them.
_PARSERS: dict[Family, _Parser] = {
    Family.JVM: _Parser(jvm.detect, jvm.parse),
    Family.NODE: _Parser(node.detect, node.parse),
    Family.PYTHON: _Parser(python.detect, python.parse),
    Family.GO: _Parser(go.detect, go.parse),
    Family.PHP: _Parser(php.detect, php.parse),
    Family.RUBY: _Parser(ruby.detect, ruby.parse),
    Family.NATIVE: _Parser(native.detect, native.parse),
}


def detect_families(text: str) -> set[Family]:
    """The families whose frames ``text`` holds."""
    return {family for family, parser in _PARSERS.items() if parser.detect(text)}


def parse_frames(text: str) -> list[Frame]:
    """The frames of every family detected in ``text``, without duplicates."""
    detected = detect_families(text)
    frames: list[Frame] = []
    seen: set[tuple[str, str, int, str | None]] = set()
    for family, parser in _PARSERS.items():
        if family not in detected:
            continue
        for frame in parser.parse(text):
            key = (frame.family, frame.file, frame.line, frame.function)
            if key not in seen:
                seen.add(key)
                frames.append(frame)
    return frames
