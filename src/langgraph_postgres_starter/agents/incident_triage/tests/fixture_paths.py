"""Whether a stack frame's file is a given file of a repository (test support).

The fixture tests and the stack-trace parser tests use the same rule. A frame names a
file as the container ran it, such as ``/app/orders/service.py``. A repository names
it relative to the repository root, such as ``orders/service.py``. The two paths name
the same file when they agree on their last ``min(len)`` path components:

- ``/app/orders/service.py`` and ``orders/service.py``;
- ``com/example/orders/OrderController.java`` (a JVM frame's package and file) and
  ``src/main/java/com/example/orders/OrderController.java``.

Node runs TypeScript as compiled JavaScript, so its frames name the compiled file.
``points_at`` compares a frame with the compiled form of a TypeScript path
(``compiled_path``). The TypeScript fixture app treats its compiled files as having the
same line numbers as its ``src/`` files, so a frame's line is a source line.
"""

from pathlib import PurePosixPath

_TYPESCRIPT_SUFFIXES = (".ts", ".tsx")


def compiled_path(path: str) -> str:
    """The compiled form of the TypeScript path ``path``. Other paths are unchanged.

    A leading ``src/`` becomes ``dist/``, and ``.ts`` or ``.tsx`` becomes ``.js``:
    ``src/orders/service.ts`` becomes ``dist/orders/service.js``.
    """
    pure = PurePosixPath(path)
    if pure.suffix not in _TYPESCRIPT_SUFFIXES:
        return path
    parts = pure.with_suffix(".js").parts
    if parts[0] == "src":
        parts = ("dist", *parts[1:])
    return PurePosixPath(*parts).as_posix()


def same_file(a: str, b: str) -> bool:
    """Whether the paths ``a`` and ``b`` agree on their last ``min(len)`` components.

    A leading ``/`` is not a component. An empty path matches nothing.
    """
    parts_a, parts_b = _components(a), _components(b)
    n = min(len(parts_a), len(parts_b))
    return n > 0 and parts_a[-n:] == parts_b[-n:]


def points_at(frame_file: str, repo_path: str) -> bool:
    """Whether a frame's file ``frame_file`` is the repository file ``repo_path``.

    A TypeScript ``repo_path`` is compared in its compiled form (``compiled_path``).
    """
    return same_file(frame_file, compiled_path(repo_path))


def _components(path: str) -> tuple[str, ...]:
    return tuple(part for part in PurePosixPath(path).parts if part != "/")
