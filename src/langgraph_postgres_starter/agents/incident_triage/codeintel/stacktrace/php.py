"""PHP uncaught exceptions (spec §9.6 item 2).

PHP prints an uncaught exception as a header, then its trace:

- the header ``PHP Fatal error:  Uncaught <class>: <message> in <file>:<line>`` (also
  without ``PHP `` or after a timestamp) names the throw site. A chained exception
  continues with ``Next <class>: <message> in <file>:<line>`` and its own trace;
- ``#<n> <file>(<line>): <function>(<arguments>)`` is a call of ``<function>`` at that
  position. A call from PHP's own code (``#<n> [internal function]: ...``) and the
  script's top level (``#<n> {main}``) have no position, so they are not frames;
- the closing ``thrown in <file> on line <n>`` repeats a header's throw site and is not
  a frame.

A ``#<n>`` line names the function it calls, so its position is in the function that
the next ``#<n>`` line names (``{main}`` for the top level), and the header's throw site
is in the function that ``#0`` names. The parser gives each frame that function: the
function whose code is at the frame's position, as for the other families. When
``#0``'s position is the header's (a PHP function such as ``intdiv`` threw), the two are
one frame. A frame with no next ``#<n>`` line has no function. A ``#0`` line with no
header before it starts a new trace, such as Laravel's ``[stacktrace]``.

Order: the header's throw site, then the ``#<n>`` positions in order; PHP prints a
chain's first exception first.

A frame is a library frame when its file contains ``/vendor/``, Composer's directory.
"""

import re

from langgraph_postgres_starter.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_postgres_starter.agents.incident_triage.schemas import Frame

# Composer's packages.
VENDOR_MARKER = "/vendor/"

# [<prefix>]Fatal error: Uncaught <exception> in <file>:<line>
_UNCAUGHT = re.compile(
    r"^(?=[^\n]*Fatal error: +Uncaught )[^\n]* in (?P<file>\S+):(?P<line>\d+)[ \t]*$",
    re.MULTILINE,
)
# Next <class>[: <message>] in <file>:<line>
_NEXT = re.compile(r"^Next [\w\\]+(?:: [^\n]*)? in (?P<file>\S+):(?P<line>\d+)[ \t]*$")
# #<n> <file>(<line>): <function>(...), #<n> [internal function]: <function>(...) or
# #<n> {main}. A function name may hold a {closure:...} part with parentheses.
_CALL = re.compile(
    r"^[ \t]*#(?P<index>\d+) (?:"
    r"(?P<file>[^\s()]+)\((?P<line>\d+)\): (?P<function>(?:[^\s(){}]|\{[^{}\n]*\})+)\("
    r"|\[internal function\]: (?P<internal>(?:[^\s(){}]|\{[^{}\n]*\})+)\("
    r"|(?P<main>\{main\})[ \t]*$)",
    re.MULTILINE,
)

_Position = tuple[str, int]


def detect(text: str) -> bool:
    """Whether ``text`` holds a PHP trace line with a position, or an uncaught header."""
    positioned = any(call["file"] is not None for call in _CALL.finditer(text))
    return positioned or _UNCAUGHT.search(text) is not None


def parse(text: str) -> list[Frame]:
    """The PHP frames in ``text``: each throw site, then its trace's positions."""
    frames: list[Frame] = []
    pending: _Position | None = None  # a position whose function the next #<n> names
    header = False  # whether pending is a header's throw site
    for line in text.splitlines():
        if (match := _UNCAUGHT.match(line) or _NEXT.match(line)) is not None:
            _flush(frames, pending)
            pending, header = (match["file"], int(match["line"])), True
            continue
        if (call := _CALL.match(line)) is None:
            continue
        if call["index"] == "0" and not header:
            _flush(frames, pending)
            pending = None
        position = (call["file"], int(call["line"])) if call["file"] is not None else None
        if pending is not None and not (header and position == pending):
            frames.append(_frame(pending, call["function"] or call["internal"] or call["main"]))
        pending, header = position, False
    _flush(frames, pending)
    return frames


def _flush(frames: list[Frame], pending: _Position | None) -> None:
    """Add the position that no #<n> line follows: its function is unknown."""
    if pending is not None:
        frames.append(_frame(pending, None))


def _frame(position: _Position, function: str | None) -> Frame:
    file, line = position
    return Frame(
        file=file,
        line=line,
        function=function,
        is_app_frame=VENDOR_MARKER not in file,
        family=Family.PHP.value,
    )
