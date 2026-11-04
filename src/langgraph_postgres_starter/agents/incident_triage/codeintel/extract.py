"""The definition around a line of source code (spec §9.6 item 4).

``enclosing_definition`` parses a file of a checkout with the grammar of its extension
(``languages.language_for``) and returns the smallest definition that contains the
line: a node of one of the language's definition types (``languages.LANGUAGES``), or,
in JavaScript and TypeScript, a binding that names a function value (ruling R60), such
as ``const handler = async (req) => {...}``.

- Lines count from 1, as in stack traces. A definition ends on the line of its last
  character that is not whitespace. Some grammars end a node later: Scala ends a
  ``def`` at column 0 of the next line, or at the indent of the next definition.
- ``symbol`` is the definition's bare name: the innermost function, method or type
  name, with no class or namespace (C++ ``PickPlanner::route_seconds`` is
  ``route_seconds``). A function value takes the name of its declarator or class
  field. It is None for a definition that has no name, and for a window.
- When no definition contains the line, the result is a window of ``WINDOW_LINES``
  lines on each side of the line, inside the file, with no symbol.
- A result longer than ``max_lines`` lines is cut to ``max_lines`` lines centred on
  the line, inside the definition or the window.
- ``text`` holds the result's lines, decoded as UTF-8 with replacement characters.

The grammar folder is ``APP_GRAMMAR_DIR`` (``get_settings().grammar_dir``): like the
library's own cache folder, it is the same for the whole process.
"""

from dataclasses import dataclass
from pathlib import Path

from tree_sitter import Node

from langgraph_postgres_starter.agents.incident_triage.codeintel.languages import (
    LanguageSpec,
    language_for,
    parse,
)
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import path_inside
from langgraph_postgres_starter.settings import get_settings

# A larger file is not read: generated or minified code, not worth parsing.
MAX_FILE_BYTES = 1_000_000
# The lines on each side of the line in a window, when no definition contains the line.
WINDOW_LINES = 20

# The code reads a point's row as point[0], never as point.row: in tree-sitter 0.26.0,
# Point.row and Point.column return the int without a new reference. Each read takes a
# reference from it, and an int above 256 (not immortal) is freed while still in use.
_ROW = 0
# Ruling R60: a declarator that names a function value spans its const, let or var
# statement, its parent. A class field spans itself.
_DECLARATOR = "variable_declarator"


@dataclass(frozen=True)
class ExtractedDef:
    """The lines ``start_line`` to ``end_line`` (from 1, inclusive) of the file ``path``.

    ``path`` is relative to the checkout's root. ``language`` is the grammar's name.
    """

    path: str
    start_line: int
    end_line: int
    symbol: str | None
    language: str
    text: str


def enclosing_definition(
    root: Path, rel_path: str, line: int, *, max_lines: int = 200
) -> ExtractedDef | None:
    """The smallest definition in ``root / rel_path`` that contains ``line``, or a window.

    Returns None when no grammar parses the file's extension (the file is not read),
    when the file is larger than ``MAX_FILE_BYTES``, and when the line is not in the
    file. Raises ValueError when ``rel_path`` leads out of ``root`` or ``max_lines`` is
    less than 1, OSError when the file cannot be read, and
    ``languages.GrammarMissingError`` when the grammar has not been fetched.
    """
    if max_lines < 1:
        raise ValueError(f"max_lines must be at least 1, not {max_lines}")
    spec = language_for(rel_path)
    if spec is None:
        return None
    source = _read(path_inside(root, rel_path))
    if source is None:
        return None
    lines = _lines(source)
    if not 1 <= line <= len(lines):
        return None
    tree = parse(spec.grammar, source, grammar_dir=get_settings().grammar_dir)
    found = _smallest_definition(tree.root_node, line, source, spec)
    if found is None:
        start, end = max(1, line - WINDOW_LINES), min(len(lines), line + WINDOW_LINES)
        symbol = None
    else:
        start, end, symbol = found
    if end - start + 1 > max_lines:
        start = min(max(start, line - (max_lines - 1) // 2), end - max_lines + 1)
        end = start + max_lines - 1
    return ExtractedDef(
        path=rel_path,
        start_line=start,
        end_line=end,
        symbol=symbol,
        language=spec.grammar,
        text="\n".join(lines[start - 1 : end]),
    )


def _read(path: Path) -> bytes | None:
    """The file's bytes, or None when it is larger than ``MAX_FILE_BYTES``."""
    with path.open("rb") as file:
        source = file.read(MAX_FILE_BYTES + 1)
    return None if len(source) > MAX_FILE_BYTES else source


def _lines(source: bytes) -> list[str]:
    """The file's lines, split at ``\\n`` only, as tree-sitter counts rows."""
    lines = source.decode("utf-8", errors="replace").split("\n")
    if lines[-1] == "":
        lines.pop()
    return [text.removesuffix("\r") for text in lines]


def _smallest_definition(
    root: Node, line: int, source: bytes, spec: LanguageSpec
) -> tuple[int, int, str | None] | None:
    """The first and last lines and the symbol of the smallest definition around ``line``.

    It visits only the nodes whose rows contain the line. Among definitions with the
    same number of lines, the one with the fewest bytes wins.
    """
    row = line - 1
    best: tuple[tuple[int, int], int, int, str | None] | None = None
    pending = [root]
    while pending:
        node = pending.pop()
        pending.extend(
            child
            for child in node.named_children
            if child.start_point[_ROW] <= row <= child.end_point[_ROW]
        )
        found = _definition(node, spec, source)
        if found is None:
            continue
        spanned, symbol = found
        start, end = _span(spanned, source)
        if start <= line <= end:
            size = (end - start, spanned.end_byte - spanned.start_byte)
            if best is None or size < best[0]:
                best = (size, start, end, symbol)
    return None if best is None else best[1:]


def _definition(node: Node, spec: LanguageSpec, source: bytes) -> tuple[Node, str | None] | None:
    """The node that spans the definition ``node`` is, and its symbol; None for no definition.

    A node of the definition types spans itself. A binding whose ``value`` is a function
    value (ruling R60) takes its name from its ``name`` field, or from ``property`` for
    a JavaScript class field.
    """
    if node.type in spec.definition_types:
        return node, _symbol(node, source)
    if node.type not in spec.binding_types:
        return None
    value = node.child_by_field_name("value")
    if value is None or value.type not in spec.function_value_types:
        return None
    name = node.child_by_field_name("name")
    if name is None:
        name = node.child_by_field_name("property")
    parent = node.parent
    spanned = parent if node.type == _DECLARATOR and parent is not None else node
    return spanned, None if name is None else _text(name, source)


def _span(node: Node, source: bytes) -> tuple[int, int]:
    """The node's first and last lines, from 1; the last holds its last non-blank byte."""
    start = node.start_point[_ROW] + 1
    text = source[node.start_byte : node.end_byte].rstrip()
    return start, start + text.count(b"\n")


def _symbol(node: Node, source: bytes) -> str | None:
    """The definition's bare name, or None when it has none.

    The name is the node at the end of the ``declarator`` and ``name`` fields: C's
    ``function_definition`` names its function through declarators, and C++'s
    ``qualified_identifier`` and Ruby's ``scope_resolution`` keep the innermost name in
    ``name``. A declarator with neither field (C++ ``int& f()``) leads to its first
    named child. The Kotlin grammar has no fields: its definitions take the first
    child whose type is an identifier.
    """
    current = node
    while True:
        child = current.child_by_field_name("declarator")
        if child is None:
            child = current.child_by_field_name("name")
        if child is None and current.type.endswith("declarator") and current.named_children:
            child = current.named_children[0]
        if child is None:
            break
        current = child
    if current is node:
        named = (c for c in node.named_children if c.type.endswith("identifier"))
        found = next(named, None)
        if found is None:
            return None
        current = found
    return _text(current, source)


def _text(node: Node, source: bytes) -> str:
    return source[node.start_byte : node.end_byte].decode("utf-8", errors="replace")
