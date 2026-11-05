"""Ruby backtraces (spec §9.6 item 2).

Ruby prints an uncaught error as ``<file>:<line>:in '<label>': <message> (<class>)``,
then one ``from <file>:<line>:in '<label>'`` line for each caller. Ruby 3.4 quotes the
label as ``'Fulfillment::LabelPrinter#address_lines'``; earlier versions as
`` `address_lines' ``. The function is the label as printed, such as
``block in <main>``. A logger's backtrace line without ``from`` is read the same way.
The code line and ``^^^^`` markers under the first line, and ``... N levels...``, are
not frames. A core method's file is ``<internal:...>``, such as ``<internal:kernel>``.

Order: the first line's position, then each ``from`` line in order (innermost first).
The backtrace of an error's cause is printed after the error's, and keeps that order.

A frame is a library frame when its file contains ``/gems/`` or ``/ruby/<n>`` (the
standard library, such as ``/usr/local/lib/ruby/3.4.0/``), or is in Ruby's core
(``<internal:``).
"""

import re

from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_and_observability.agents.incident_triage.schemas import Frame

# Installed gems.
GEMS_MARKER = "/gems/"
# The standard library: /ruby/ and a version.
_STDLIB = re.compile(r"/ruby/\d")
# Core methods.
CORE_PREFIX = "<internal:"

# [from ]<file>:<line>:in '<label>' (or `<label>')
_FRAME = re.compile(
    r"^[ \t]*(?:from )?(?P<file>\S+?):(?P<line>\d+):in [`'](?P<function>[^'\n]+)'",
    re.MULTILINE,
)


# Each stack trace parser has this shape: detect() and parse() (AGENTS.md).
# pylint: disable=duplicate-code
def detect(text: str) -> bool:
    """Whether ``text`` holds a Ruby backtrace line."""
    return _FRAME.search(text) is not None


def parse(text: str) -> list[Frame]:
    """The Ruby frames in ``text``, in the order printed (innermost first)."""
    return [
        Frame(
            file=match["file"],
            line=int(match["line"]),
            function=match["function"],
            is_app_frame=not _is_library(match["file"]),
            family=Family.RUBY.value,
        )
        for match in _FRAME.finditer(text)
    ]


def _is_library(file: str) -> bool:
    return GEMS_MARKER in file or file.startswith(CORE_PREFIX) or _STDLIB.search(file) is not None
