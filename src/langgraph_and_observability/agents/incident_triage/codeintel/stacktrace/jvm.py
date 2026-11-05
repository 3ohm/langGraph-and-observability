"""JVM stack traces: Java, Kotlin and Scala (spec §9.6 item 2).

A frame is ``at [<loader>/][<module>/]<class>.<method>(<file>:<line>)``, maybe followed
by a jar suffix such as Spring Boot's ``~[spring-boot-3.5.7.jar:3.5.7]``. Its file is
the class's package path plus the file name that the frame prints:
``com.example.orders.OrderService.ship(OrderService.java:35)`` is in
``com/example/orders/OrderService.java``. The package is the class name up to its last
dot, so the suffixes of inner classes, lambdas and companions (``$1``, ``$$anonfun$1``,
``$Companion``) leave it unchanged, and a Kotlin file class such as ``MainKt`` is in the
file that the frame names (``Main.kt``). The function is ``<class>.<method>`` as
printed. A frame that prints no line (``Native Method``, ``Unknown Source``, or a file
with no line) names no location and is skipped, so it is never an app frame.

A frame is a library frame when its class starts with one of ``LIBRARY_PREFIXES``.

Order: Java prints an exception's frames innermost first, then each cause in a block
that starts with an unindented ``Caused by:`` line. Any unindented line that is not a
frame (an exception's first line, ``Caused by:`` or other log text) ends a block. The
parser returns the blocks last first, so the deepest cause comes first, and keeps the
order of the frames in each block. The frames that ``... N more`` leaves out are not
repeated. A suppressed exception, and its own causes, are printed indented, so their
frames stay in the block of the exception they belong to, after its frames.
"""

import re

from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_and_observability.agents.incident_triage.schemas import Frame

# The packages of the JVM, of Kotlin, Scala and Akka, and of Spring and Apache.
LIBRARY_PREFIXES = (
    "java.",
    "javax.",
    "jdk.",
    "sun.",
    "kotlin.",
    "kotlinx.",
    "scala.",
    "akka.",
    "org.springframework.",
    "org.apache.",
)

# at [<loader>/][<module>[@<version>]/]<class>.<method>(<file>:<line>)
_FRAME = re.compile(
    r"^[ \t]*at (?:[^\s/()]*/){0,2}"
    r"(?P<cls>[\w$.]+)\.(?P<method>[\w$<>-]+)"
    r"\((?P<file>[^\s():]+):(?P<line>\d+)\)",
    re.MULTILINE,
)


def detect(text: str) -> bool:
    """Whether ``text`` holds a JVM frame with a line."""
    return _FRAME.search(text) is not None


def parse(text: str) -> list[Frame]:
    """The JVM frames in ``text``, the deepest cause's frames first."""
    blocks: list[list[Frame]] = []
    block: list[Frame] | None = None
    for line in text.splitlines():
        match = _FRAME.match(line)
        if match is not None:
            if block is None:
                block = []
                blocks.append(block)
            block.append(_frame(match))
        elif not line[:1].isspace():
            block = None
    return [frame for block in reversed(blocks) for frame in block]


def _frame(match: re.Match[str]) -> Frame:
    cls = match["cls"]
    package = cls.rpartition(".")[0]
    file = f"{package.replace('.', '/')}/{match['file']}" if package else match["file"]
    return Frame(
        file=file,
        line=int(match["line"]),
        function=f"{cls}.{match['method']}",
        is_app_frame=not cls.startswith(LIBRARY_PREFIXES),
        family=Family.JVM.value,
    )
