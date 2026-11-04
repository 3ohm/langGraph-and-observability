"""Map stack-frame files to the files of a checkout (spec §9.6 item 3).

A frame names a file as the container ran it, such as ``/app/orders/service.py``. A
checkout names it relative to the repository root, such as ``orders/service.py``.
``FileIndex.resolve`` picks the repository file that shares the longest suffix of
whole path components with the frame's file, the file name at least. Among files with
equally long suffixes, the shortest path wins: the fewest components, then the fewest
characters, then the first in name order.

Some frames name their file in another form than the repository does, so the index
also tries other forms of the frame's file. A longer match wins whatever its form; on
an equal match, the form that the frame prints wins:

- A JVM frame's file is its package path (``com/example/orders/Main.java``, as the
  JVM parser writes it). When the file has no folder, the package comes from the
  class in the frame's function, when the frame has one.
- A Node frame names the compiled JavaScript. The index also tries the TypeScript
  source: a ``dist`` folder becomes ``src`` and ``.js`` becomes ``.ts`` or ``.tsx``,
  so ``/app/dist/orders/service.js`` finds ``src/orders/service.ts``.

``FileIndex.build`` indexes the regular files below the root. It skips ``.git``
folders and symbolic links, so a resolved path never leads out of the checkout, and
it stops after ``max_files`` files, which ``truncated`` records.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from langgraph_postgres_starter.agents.incident_triage.codeintel.stacktrace.family import Family
from langgraph_postgres_starter.agents.incident_triage.schemas import Frame

# The most files that FileIndex.build indexes. A source repository has far fewer.
DEFAULT_MAX_FILES = 200_000

_SKIPPED_DIRS = frozenset({".git"})
# The TypeScript sources that a compiled JavaScript file can come from.
_JS_SOURCE_SUFFIXES = (".ts", ".tsx")

type _Parts = tuple[str, ...]


@dataclass(frozen=True)
class FileIndex:
    """The files of a checkout, by file name. Build it with ``FileIndex.build``.

    ``file_count`` is the number of files indexed; ``truncated`` is True when the
    checkout had more than ``max_files`` files and the rest were left out.
    """

    file_count: int
    truncated: bool
    _by_name: dict[str, list[_Parts]]

    @classmethod
    def build(cls, root: Path, *, max_files: int = DEFAULT_MAX_FILES) -> Self:
        """Index the regular files below ``root``, in name order, at most ``max_files``.

        Raises OSError (FileNotFoundError, NotADirectoryError) when ``root`` is not a
        folder that can be read.
        """
        by_name: dict[str, list[_Parts]] = {}
        count = 0
        # Folders still to read, as paths relative to root; the last one is read next.
        pending: list[_Parts] = [()]
        while pending:
            folder = pending.pop()
            with os.scandir(root.joinpath(*folder)) as scan:
                entries = sorted(scan, key=lambda entry: entry.name)
            subfolders: list[_Parts] = []
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    if entry.name not in _SKIPPED_DIRS:
                        subfolders.append((*folder, entry.name))
                elif entry.is_file(follow_symlinks=False):
                    if count == max_files:
                        return cls(file_count=count, truncated=True, _by_name=by_name)
                    by_name.setdefault(entry.name, []).append((*folder, entry.name))
                    count += 1
            pending.extend(reversed(subfolders))
        return cls(file_count=count, truncated=False, _by_name=by_name)

    def resolve(self, frame: Frame) -> str | None:
        """The path, relative to the root, of the file that ``frame`` names, or None."""
        best: tuple[int, int, int, int, str] | None = None
        for rank, candidate in enumerate(_forms(frame)):
            for path in self._by_name.get(candidate[-1], ()):
                matched = _common_suffix(candidate, path)
                joined = "/".join(path)
                # Smallest wins: the longest match, then the form the frame prints,
                # then the shortest path.
                key = (-matched, rank, len(path), len(joined), joined)
                if best is None or key < best:
                    best = key
        return None if best is None else best[-1]


def _parts(file: str) -> _Parts:
    """The components of a path; empty and ``.`` components are dropped."""
    return tuple(part for part in file.split("/") if part not in ("", "."))


def _forms(frame: Frame) -> list[_Parts]:
    """The forms of the frame's file to look up, the form the frame prints first."""
    parts = _parts(frame.file)
    if not parts:
        return []
    forms = [parts]
    if frame.family == Family.JVM and len(parts) == 1 and frame.function is not None:
        # com.example.orders.OrderService$1.ship: the class, then its package.
        package = frame.function.rpartition(".")[0].rpartition(".")[0]
        if package:
            forms.append((*package.split("."), parts[0]))
    if frame.family == Family.NODE and parts[-1].endswith(".js"):
        folder, name = parts[:-1], parts[-1]
        names = [name, *(name.removesuffix(".js") + s for s in _JS_SOURCE_SUFFIXES)]
        folders = [folder]
        if "dist" in folder:
            last = len(folder) - 1 - folder[::-1].index("dist")
            folders.append((*folder[:last], "src", *folder[last + 1 :]))
        for form in ((*f, n) for f in folders for n in names):
            if form not in forms:
                forms.append(form)
    return forms


def _common_suffix(a: _Parts, b: _Parts) -> int:
    """The number of trailing components that ``a`` and ``b`` share."""
    n = 0
    for x, y in zip(reversed(a), reversed(b), strict=False):
        if x != y:
            break
        n += 1
    return n
