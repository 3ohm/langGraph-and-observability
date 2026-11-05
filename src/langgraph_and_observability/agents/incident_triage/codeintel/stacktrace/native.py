"""Native C and C++ backtraces: symbolized sanitizer output (spec §9.6 item 2).

AddressSanitizer and the other sanitizers print a frame as
``#<n> 0x<pc> in <function> <file>:<line>[:<column>]``; the function is printed as is,
with its parameters, such as ``warehouse::PickPlanner::route_seconds(...) const``.
Calls that the compiler inlined print one frame each, all with the same PC. A frame
that names only a module, ``#<n> 0x<pc> [in <function>] (<module>+0x<offset>)``, or
only a function, names no source position and is skipped, as a JVM frame without a
line is. ``SUMMARY:`` and the other report lines are not frames. Native crashes often
print no trace at all (a bare ``Segmentation fault``): then there are no frames.

Order: each stack is printed from ``#0``, the innermost frame. A report with more than
one stack (``freed by thread T0 here:``) keeps the stacks in print order.

A frame is a library frame (ruling R56) when its file:

- is under one of ``SYSTEM_DIRS``, the system's headers and libraries (the standard
  library's headers, such as ``/usr/include/c++/12/bits/stl_vector.h``);
- has a directory in ``RUNTIME_DIRS``, a compiler or sanitizer runtime's sources, such
  as ``../../../../src/libstdc++-v3/libsupc++/eh_throw.cc``;
- is in glibc's sources: a directory that starts with ``glibc-``, or a relative path
  whose first directory is in ``GLIBC_DIRS``. glibc's build prints its files relative
  to its source tree, such as ``nptl/pthread_kill.c`` or ``../sysdeps/posix/raise.c``.
"""

import re

from langgraph_and_observability.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_and_observability.agents.incident_triage.schemas import Frame

# The system's headers and libraries. Each counts only at the start of the file.
SYSTEM_DIRS = ("/usr/include/", "/usr/lib/", "/lib/", "/usr/local/include/")
# The sources of GCC's and LLVM's runtimes: the C++ standard libraries, the sanitizers
# and the compiler's support library.
RUNTIME_DIRS = frozenset(
    {
        "libstdc++-v3",
        "libsanitizer",
        "sanitizer_common",
        "compiler-rt",
        "libgcc",
        "libcxx",
        "libcxxabi",
    }
)
# The top directories of glibc's sources that its functions in crash stacks come from
# (raise, abort, __assert_fail, __libc_start_main, malloc, memcpy, printf...).
GLIBC_DIRS = frozenset(
    {
        "assert",
        "csu",
        "dlfcn",
        "elf",
        "libio",
        "malloc",
        "misc",
        "nptl",
        "posix",
        "signal",
        "stdio-common",
        "stdlib",
        "string",
        "sysdeps",
    }
)
# glibc's source tree in a build directory, such as /build/glibc-SzIz7B/glibc-2.31/.
GLIBC_PREFIX = "glibc-"

# #<n> 0x<pc> in <function> <file>:<line>[:<column>]
_FRAME = re.compile(
    r"^[ \t]*#\d+ 0x[0-9a-fA-F]+ in (?P<function>[^\n]+) "
    r"(?P<file>[^\s()]+?):(?P<line>\d+)(?::\d+)?[ \t]*$",
    re.MULTILINE,
)


def detect(text: str) -> bool:
    """Whether ``text`` holds a symbolized native frame."""
    return _FRAME.search(text) is not None


def parse(text: str) -> list[Frame]:
    """The symbolized native frames in ``text``, in the order printed (``#0`` first)."""
    return [
        Frame(
            file=match["file"],
            line=int(match["line"]),
            function=match["function"],
            is_app_frame=not _is_library(match["file"]),
            family=Family.NATIVE.value,
        )
        for match in _FRAME.finditer(text)
    ]


def _is_library(file: str) -> bool:
    if file.startswith(SYSTEM_DIRS):
        return True
    dirs = [part for part in file.split("/")[:-1] if part not in ("", ".", "..")]
    if any(part in RUNTIME_DIRS or part.startswith(GLIBC_PREFIX) for part in dirs):
        return True
    return not file.startswith("/") and bool(dirs) and dirs[0] in GLIBC_DIRS
