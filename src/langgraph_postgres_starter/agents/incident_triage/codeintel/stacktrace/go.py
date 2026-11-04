"""Go panics and goroutine traces (spec §9.6 item 2).

A trace is a block per goroutine. The block starts with a header such as
``goroutine 23 [running]:`` (``goroutine 1 gp=0xc000002380 m=0 mp=0x5a2f20 [running]:``
with ``GOTRACEBACK=system`` and for a fatal runtime error) and ends at a blank line or
at any other line that is not part of a trace. In a block, a frame is two lines:

- the function and its arguments, such as
  ``github.com/example/orders-api/internal/orders.(*Service).reserve(0xc0001b2000, {...})``,
  or ``main.f(...)`` for an inlined call. The function is the line without its argument
  list;
- a tab, then ``<file>:<line>``, maybe followed by the PC offset (``+0x14c``) and, with
  ``GOTRACEBACK=system``, by ``fp=``, ``sp=`` and ``pc=`` values.

A block can end with ``created by <function> [in goroutine <n>]`` and a position: the
``go`` statement that started the goroutine. It is kept as a frame of that function,
after the goroutine's own frames. ``...additional frames elided...`` is not a frame.
Function and position lines outside a block are not frames.

Order: Go prints the panicking goroutine first, innermost frame first, and then, in a
full dump, the other goroutines. The parser keeps that print order.

A frame is a library frame when its file is in GOROOT (``/usr/local/go/src/``), is the
runtime of a ``-trimpath`` build (it starts with ``runtime/``), is in a ``vendor/``
directory, or is in the module cache (``go/pkg/mod/``).
"""

import re

from langgraph_postgres_starter.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_postgres_starter.agents.incident_triage.schemas import Frame

# GOROOT in the official golang images.
GOROOT_PREFIX = "/usr/local/go/src/"
# The runtime's files in a -trimpath build. It counts only at the start of the file, so
# an app package named runtime stays an app package.
RUNTIME_PREFIX = "runtime/"
# Vendored packages and the module cache, as whole directories.
LIBRARY_DIRS = ("vendor/", "go/pkg/mod/")

# goroutine <n>[ <key>=<value>...] [<state>]:
_GOROUTINE = re.compile(r"^goroutine \d+(?: [^\s\[]+)* \[[^\]\n]*\]:[ \t]*$", re.MULTILINE)
# <function>(<arguments>): the arguments hold no parentheses.
_CALL = re.compile(r"^(?P<function>[^\s(][^\n]*?)\([^()\n]*\)[ \t]*$")
# created by <function>[ in goroutine <n>]
_CREATED_BY = re.compile(r"^created by (?P<function>\S+)(?: in goroutine \d+)?[ \t]*$")
# <tab><file>:<line>[ +0x<offset>][ fp=0x... sp=0x... pc=0x...]
_POSITION = re.compile(
    r"^[ \t]+(?P<file>\S[^\n]*?):(?P<line>\d+)"
    r"(?: \+0x[0-9a-f]+)?(?: (?:fp|sp|pc)=0x[0-9a-f]+)*[ \t]*$",
    re.MULTILINE,
)
# ...additional frames elided... and ...<n> frames elided...
_ELIDED = re.compile(r"^[ \t]*\.\.\.(?:additional|\d+) frames elided\.\.\.[ \t]*$")


def detect(text: str) -> bool:
    """Whether ``text`` holds a goroutine header and a frame position."""
    return _GOROUTINE.search(text) is not None and _POSITION.search(text) is not None


def parse(text: str) -> list[Frame]:
    """The Go frames in ``text``, in the order printed (innermost first)."""
    frames: list[Frame] = []
    in_goroutine = False
    function: str | None = None  # the function line waiting for its position
    for line in text.splitlines():
        if _GOROUTINE.match(line):
            in_goroutine, function = True, None
            continue
        if not in_goroutine:
            continue
        if (position := _POSITION.match(line)) is not None:
            if function is not None:
                frames.append(_frame(function, position))
                function = None
        elif (call := _CREATED_BY.match(line) or _CALL.match(line)) is not None:
            function = call["function"]
        elif not _ELIDED.match(line):
            in_goroutine, function = False, None
    return frames


def _frame(function: str, position: re.Match[str]) -> Frame:
    file = position["file"]
    return Frame(
        file=file,
        line=int(position["line"]),
        function=function,
        is_app_frame=not _is_library(file),
        family=Family.GO.value,
    )


def _is_library(file: str) -> bool:
    if file.startswith((GOROOT_PREFIX, RUNTIME_PREFIX, *LIBRARY_DIRS)):
        return True
    return any(f"/{directory}" in file for directory in LIBRARY_DIRS)
