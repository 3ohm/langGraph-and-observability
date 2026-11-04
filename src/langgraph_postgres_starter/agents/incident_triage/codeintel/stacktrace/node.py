"""Node.js stack traces: JavaScript and TypeScript (spec §9.6 item 2).

V8 prints an error's frames innermost first, one per line:

- ``at <function> (<file>:<line>:<column>)``;
- ``at <file>:<line>:<column>`` for an anonymous function (the function is None);
- either form after ``async`` for an awaited call, such as
  ``at async SettlementService.flush (/app/dist/orders/service.js:48:34)``. The
  function is the name without ``async``.

An ES module's file is a ``file://`` URL; the frame's file is its path. A frame with no
file and line, such as ``at Array.map (<anonymous>)``, is skipped, and so are the file,
source line and caret that Node prints above an uncaught error. TypeScript runs as
compiled JavaScript, so its frames name the compiled files.

A frame is a library frame when its file contains one of ``LIBRARY_MARKERS``.
"""

import re

from langgraph_postgres_starter.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_postgres_starter.agents.incident_triage.schemas import Frame

# Node's own modules, and installed packages.
LIBRARY_MARKERS = ("node:internal", "node_modules")

# at [async ][<function> (]<file>:<line>:<column>[)]
_FRAME = re.compile(
    r"^[ \t]*at (?:async )?(?:(?P<function>.+?) \()?"
    r"(?:file://)?(?P<file>[^\s()]+?):(?P<line>\d+):\d+\)?\s*$",
    re.MULTILINE,
)


def detect(text: str) -> bool:
    """Whether ``text`` holds a Node frame."""
    return _FRAME.search(text) is not None


def parse(text: str) -> list[Frame]:
    """The Node frames in ``text``, in the order printed (innermost first)."""
    return [
        Frame(
            file=match["file"],
            line=int(match["line"]),
            function=match["function"],
            is_app_frame=not any(marker in match["file"] for marker in LIBRARY_MARKERS),
            family=Family.NODE.value,
        )
        for match in _FRAME.finditer(text)
    ]
