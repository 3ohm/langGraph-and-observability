"""Python tracebacks (spec §9.6 item 2).

A frame is the line ``File "<file>", line <n>, in <function>``; a frame of code that
failed to compile has no ``, in <function>`` (the function is None). The code line,
the ``~~~^^^`` markers and ``...<N lines>...`` below a frame are not frames. The
frames of an exception group, printed after ``|``, are read the same way.

Python prints the innermost frame last, so the parser reverses the frames. When an
exception occurs while another is handled ("During handling of the above exception,
another exception occurred:"), or is raised from it, the last traceback is printed
last: so the reversed frames are the last traceback's, innermost first, then each
earlier traceback's, innermost first.

A frame is a library frame when its file contains one of ``LIBRARY_MARKERS``.
"""

import re

from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_and_observability.agents.incident_triage.schemas import Frame

# Installed packages, frozen modules and the standard library.
LIBRARY_MARKERS = (
    "site-packages",
    "dist-packages",
    "<frozen",
    "/usr/lib/python3",
    "/usr/local/lib/python3.",
)

# File "<file>", line <n>[, in <function>]
_FRAME = re.compile(
    r'^[ \t|]*File "(?P<file>[^"\n]+)", line (?P<line>\d+)(?:, in (?P<function>[^\n]+?))?\s*$',
    re.MULTILINE,
)


def detect(text: str) -> bool:
    """Whether ``text`` holds a Python traceback frame."""
    return _FRAME.search(text) is not None


def parse(text: str) -> list[Frame]:
    """The Python frames in ``text``, the last traceback's innermost frame first."""
    frames = [
        Frame(
            file=match["file"],
            line=int(match["line"]),
            function=match["function"],
            is_app_frame=not any(marker in match["file"] for marker in LIBRARY_MARKERS),
            family=Family.PYTHON.value,
        )
        for match in _FRAME.finditer(text)
    ]
    return frames[::-1]
