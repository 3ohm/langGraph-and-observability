"""The languages that code evidence parses, and their tree-sitter grammars (spec §9.6 item 4).

``LANGUAGES`` holds one ``LanguageSpec`` per grammar: the file extensions that it parses
and its definition node types, the nodes that ``extract.enclosing_definition`` returns.
A definition is a named function, method, constructor or class-like type (interface,
enum, record, struct, trait, object, module):

- Anonymous functions (lambdas, closures, arrow functions, blocks) are not definitions.
  A line inside one belongs to the named definition around it.
- Except in JavaScript and TypeScript (ruling R60): a function value (an arrow function,
  a function expression or a generator function) that is the ``value`` of a binding (a
  ``const``, ``let`` or ``var`` declarator, or a class field) is a definition, named by
  the binding. Its span is the whole ``const``/``let``/``var`` statement, or the field.
  These are a spec's ``function_value_types`` and ``binding_types``.
- Wrappers such as Python's ``decorated_definition``, C++'s ``template_declaration``
  and JavaScript's ``export_statement`` are not definitions either: a definition starts
  at its ``def``, its return type or its ``function``, not at the decorator above it.
  Java and Kotlin annotations are part of the definition's modifiers, so there a
  definition starts at its first annotation.
- C and C++ list ``function_definition`` only. Their ``struct_specifier`` (and C++'s
  ``class_specifier``) is also the node of a type that a declaration only names, as in
  ``const struct warehouse *find_warehouse(int id)``.
- Namespaces and packages are not definitions: they span whole files.

Grammars are fetched at setup or image build time, never while the agent runs (ruling
R51). tree-sitter-language-pack downloads a grammar on its first use, so ``parse`` checks
first that the grammar is on disk, and raises ``GrammarMissingError`` when it is not.
``FETCH_COMMAND`` (``make grammars``) fetches the grammars into ``APP_GRAMMAR_DIR``, or
into the library's user cache when that is not set.

The library keeps one cache folder per process, which ``configure`` sets: ``parse``
sets it to the folder that it is given on each call that loads a grammar. Parsers are
cached per grammar and folder. A parser is not thread-safe, so each one has a lock.
"""

import argparse
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType

import tree_sitter_language_pack as tslp
from tree_sitter import Parser, Tree

from langgraph_postgres_starter.settings import get_settings

# The command that fetches the grammars. make grammars runs it.
FETCH_COMMAND = (
    "python -m langgraph_postgres_starter.agents.incident_triage.codeintel.languages --download"
)

_JS_DEFINITIONS = frozenset(
    {
        "function_declaration",
        "generator_function_declaration",
        "method_definition",
        "class_declaration",
    }
)
_TS_DEFINITIONS = _JS_DEFINITIONS | {
    "abstract_class_declaration",
    "interface_declaration",
    "enum_declaration",
}
# Ruling R60: the function values that a binding can name, and the bindings. The
# JavaScript grammar names a class field field_definition, the TypeScript grammars
# public_field_definition.
_FUNCTION_VALUES = frozenset({"arrow_function", "function_expression", "generator_function"})
_JS_BINDINGS = frozenset({"variable_declarator", "field_definition"})
_TS_BINDINGS = frozenset({"variable_declarator", "public_field_definition"})


@dataclass(frozen=True)
class LanguageSpec:
    """A tree-sitter grammar, the file extensions it parses and its definition node types.

    A node of ``function_value_types`` that is the ``value`` of a node of
    ``binding_types`` is a definition too (ruling R60). Both are empty except for
    JavaScript and TypeScript.
    """

    grammar: str
    extensions: tuple[str, ...]
    definition_types: frozenset[str]
    function_value_types: frozenset[str] = frozenset()
    binding_types: frozenset[str] = frozenset()


def _spec(
    grammar: str,
    extensions: tuple[str, ...],
    definitions: Iterable[str],
    *,
    function_values: Iterable[str] = (),
    bindings: Iterable[str] = (),
) -> LanguageSpec:
    return LanguageSpec(
        grammar, extensions, frozenset(definitions), frozenset(function_values), frozenset(bindings)
    )


# One entry per grammar, by grammar name.
LANGUAGES: Mapping[str, LanguageSpec] = MappingProxyType(
    {
        spec.grammar: spec
        for spec in (
            _spec(
                "javascript",
                (".js", ".mjs", ".cjs", ".jsx"),
                _JS_DEFINITIONS,
                function_values=_FUNCTION_VALUES,
                bindings=_JS_BINDINGS,
            ),
            _spec(
                "typescript",
                (".ts",),
                _TS_DEFINITIONS,
                function_values=_FUNCTION_VALUES,
                bindings=_TS_BINDINGS,
            ),
            _spec(
                "tsx",
                (".tsx",),
                _TS_DEFINITIONS,
                function_values=_FUNCTION_VALUES,
                bindings=_TS_BINDINGS,
            ),
            _spec(
                "java",
                (".java",),
                {
                    "method_declaration",
                    "constructor_declaration",
                    "compact_constructor_declaration",
                    "class_declaration",
                    "interface_declaration",
                    "enum_declaration",
                    "record_declaration",
                    "annotation_type_declaration",
                },
            ),
            _spec(
                "kotlin",
                (".kt", ".kts"),
                {"function_declaration", "class_declaration", "object_declaration"},
            ),
            _spec(
                "scala",
                (".scala", ".sc"),
                {
                    "function_definition",
                    "class_definition",
                    "object_definition",
                    "trait_definition",
                    "enum_definition",
                },
            ),
            _spec("python", (".py",), {"function_definition", "class_definition"}),
            _spec("go", (".go",), {"function_declaration", "method_declaration", "type_spec"}),
            _spec(
                "php",
                (".php",),
                {
                    "function_definition",
                    "method_declaration",
                    "class_declaration",
                    "interface_declaration",
                    "trait_declaration",
                    "enum_declaration",
                },
            ),
            _spec("ruby", (".rb",), {"method", "singleton_method", "class", "module"}),
            _spec("c", (".c", ".h"), {"function_definition"}),
            _spec("cpp", (".cpp", ".cc", ".cxx", ".hpp", ".hh"), {"function_definition"}),
        )
    }
)

_BY_EXTENSION = {ext: spec for spec in LANGUAGES.values() for ext in spec.extensions}


class GrammarMissingError(RuntimeError):
    """A grammar is not on disk. It names the grammar, the folder and ``FETCH_COMMAND``."""

    def __init__(self, grammar: str, folder: str) -> None:
        self.grammar = grammar
        super().__init__(
            f"the tree-sitter grammar {grammar!r} is not in {folder}, and the agent never "
            f"downloads one at run time. Fetch the grammars at setup time: make grammars "
            f"({FETCH_COMMAND}), with the APP_GRAMMAR_DIR that the agent uses"
        )


def language_for(path: str) -> LanguageSpec | None:
    """The language of the file ``path``, by its extension (case-sensitive), or None."""
    return _BY_EXTENSION.get(PurePosixPath(path).suffix)


def parse(grammar: str, source: bytes, *, grammar_dir: Path | None) -> Tree:
    """Parse ``source`` with ``grammar``, loaded from ``grammar_dir`` (None: the user cache).

    Raises ValueError for a grammar that is not in ``LANGUAGES``, and GrammarMissingError
    when the grammar is not in the folder. It never downloads a grammar.
    """
    entry = _parser(grammar, grammar_dir)
    with entry.lock:
        return entry.parser.parse(source)


def download_grammars(grammar_dir: Path | None) -> str:
    """Download the grammars of ``LANGUAGES`` into ``grammar_dir``; return their folder.

    Only setup and image builds call it (``FETCH_COMMAND``). Raises RuntimeError when a
    grammar is still missing afterwards.
    """
    with _lock:
        _configure(grammar_dir)
        tslp.download(list(LANGUAGES))
        missing = sorted(set(LANGUAGES) - set(tslp.downloaded_languages()))
        if missing:
            raise RuntimeError(f"the tree-sitter grammars {missing} did not download")
        return tslp.cache_dir()


@dataclass(frozen=True)
class _CachedParser:
    parser: Parser
    lock: threading.Lock


# Guards the library's cache folder, _base, _default_base and _parsers.
_lock = threading.Lock()
_parsers: dict[tuple[str, Path | None], _CachedParser] = {}
# The base folder that the library is configured with, and its default base folder.
# Both are None until the first call to _configure.
_base: Path | None = None
_default_base: Path | None = None


def _parser(grammar: str, grammar_dir: Path | None) -> _CachedParser:
    if grammar not in LANGUAGES:
        raise ValueError(f"{grammar!r} is not a grammar of LANGUAGES: {', '.join(LANGUAGES)}")
    key = (grammar, grammar_dir)
    with _lock:
        entry = _parsers.get(key)
        if entry is None:
            _configure(grammar_dir)
            # The library would download a missing grammar here: check first.
            if grammar not in tslp.downloaded_languages():
                raise GrammarMissingError(grammar, tslp.cache_dir())
            entry = _CachedParser(tslp.get_parser(grammar), threading.Lock())
            _parsers[key] = entry
        return entry


def _configure(grammar_dir: Path | None) -> None:
    """Point the library at ``grammar_dir``, or at its default folder when None.

    The library keeps its grammars in ``<base>/tree-sitter-language-pack/v<version>/libs``
    and has no call that restores its default base, so the first call records it.
    """
    global _base, _default_base
    if _default_base is None:
        _default_base = _base = Path(tslp.cache_dir()).parents[2]
    base = _default_base if grammar_dir is None else grammar_dir
    if base != _base:
        tslp.configure(tslp.PackConfig(cache_dir=str(base)))
        _base = base


def main(argv: Sequence[str] | None = None) -> int:
    """``FETCH_COMMAND``: download the grammars into ``APP_GRAMMAR_DIR`` (or the user cache)."""
    parser = argparse.ArgumentParser(
        prog="python -m langgraph_postgres_starter.agents.incident_triage.codeintel.languages",
        description=(
            "Fetch the tree-sitter grammars of code evidence into APP_GRAMMAR_DIR, or into "
            "the library's user cache when it is not set. Run it at setup or image build "
            "time: the agent never downloads a grammar."
        ),
    )
    parser.add_argument(
        "--download", action="store_true", required=True, help="download the grammars"
    )
    parser.parse_args(argv)
    folder = download_grammars(get_settings().grammar_dir)
    print(f"{len(LANGUAGES)} tree-sitter grammars in {folder}: {', '.join(LANGUAGES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
