"""tree-sitter extraction: the definition around a line of source code (spec §9.6 item 4).

Each app fixture's expected code location is the definition that encloses each of its
lines. The other tests cover the window fallback, unknown extensions, truncation, bare
symbols, the tsx grammar (no fixture app uses it) and ruling R51: grammars are fetched at
setup time, never at run time.
"""

import textwrap
from pathlib import Path

import pytest
import tree_sitter_language_pack

from langgraph_postgres_starter.agents.incident_triage.codeintel import languages
from langgraph_postgres_starter.agents.incident_triage.codeintel.extract import (
    ExtractedDef,
    enclosing_definition,
)
from langgraph_postgres_starter.agents.incident_triage.codeintel.languages import (
    LANGUAGES,
    GrammarMissingError,
    language_for,
)
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import (
    list_fixtures,
    load_fixture,
)
from langgraph_postgres_starter.settings import get_settings

# The library functions that download a grammar. get_parser and get_language download a
# grammar that is missing, too.
_DOWNLOADERS = ("download", "download_all", "download_group", "prefetch", "init")
_LOADERS = ("get_parser", "get_language")


def _app_fixtures() -> list[str]:
    """The IDs of the incidents that have an app."""
    return [fid for fid in list_fixtures() if load_fixture(fid).source is not None]


def _app(name: str) -> Path:
    return get_settings().fixtures_dir / "apps" / name


def _lines(path: Path) -> list[str]:
    return path.read_text().split("\n")


def _no_download(*args: object, **kwargs: object) -> None:
    raise AssertionError("a tree-sitter grammar was downloaded or loaded at run time")


@pytest.mark.parametrize("fid", _app_fixtures())
def test_enclosing_definition_per_language(fid):
    f = load_fixture(fid)
    assert f.source is not None
    code = f.expected["code"]
    app = _app(f.source["app"])
    spec = language_for(code["path"])
    assert spec is not None
    lines = _lines(app / code["path"])
    expected = ExtractedDef(
        path=code["path"],
        start_line=code["start_line"],
        end_line=code["end_line"],
        symbol=code["symbol"],
        language=spec.grammar,
        text="\n".join(lines[code["start_line"] - 1 : code["end_line"]]),
    )
    for line in range(code["start_line"], code["end_line"] + 1):
        assert enclosing_definition(app, code["path"], line) == expected, f"line {line}"


def test_window_fallback_top_level():
    app = _app("python")
    lines = _lines(app / "orders/service.py")
    # Line 3, an import, is in no definition: 20 lines on each side, cut at line 1.
    d = enclosing_definition(app, "orders/service.py", 3)
    assert d is not None
    assert (d.start_line, d.end_line, d.symbol) == (1, 23, None)
    assert d.text == "\n".join(lines[0:23])
    # Line 30, between a class and a function: cut at the last line, 34.
    d = enclosing_definition(app, "orders/service.py", 30)
    assert d is not None
    assert (d.start_line, d.end_line, d.symbol, d.language) == (10, 34, None, "python")
    assert d.text == "\n".join(lines[9:34])


@pytest.mark.parametrize(
    "name", ["README.md", "Makefile", "src/lib.rs", "orders/service.pyc", "orders/SERVICE.PY"]
)
def test_unknown_extension_none(tmp_path, name):
    # The file is never read, so it need not exist.
    assert language_for(name) is None
    assert enclosing_definition(tmp_path, name, 1) is None


def test_truncation_centred(tmp_path):
    body = [f"    total += {i}" for i in range(300)]
    source = "\n".join(["def long_function(total):", *body, "    return total", ""])
    (tmp_path / "long.py").write_text(source)
    lines = source.split("\n")
    # 302 lines: the def line, 300 body lines and the return line.
    d = enclosing_definition(tmp_path, "long.py", 150, max_lines=21)
    assert d is not None
    assert (d.start_line, d.end_line, d.symbol) == (140, 160, "long_function")
    assert d.text == "\n".join(lines[139:160])
    # Near either end, the cut stays inside the definition.
    d = enclosing_definition(tmp_path, "long.py", 3, max_lines=21)
    assert d is not None and (d.start_line, d.end_line) == (1, 21)
    d = enclosing_definition(tmp_path, "long.py", 300, max_lines=21)
    assert d is not None and (d.start_line, d.end_line) == (282, 302)
    d = enclosing_definition(tmp_path, "long.py", 150)
    assert d is not None and (d.start_line, d.end_line) == (51, 250)
    # A window is cut the same way.
    d = enclosing_definition(_app("python"), "orders/service.py", 3, max_lines=5)
    assert d is not None and (d.start_line, d.end_line, d.symbol) == (1, 5, None)
    with pytest.raises(ValueError, match="max_lines"):
        enclosing_definition(tmp_path, "long.py", 150, max_lines=0)


def test_line_outside_the_file_none():
    app = _app("python")
    assert enclosing_definition(app, "orders/service.py", 0) is None
    assert enclosing_definition(app, "orders/service.py", 35) is None


def test_path_outside_root_rejected():
    with pytest.raises(ValueError, match="inside"):
        enclosing_definition(_app("python"), "../go/internal/orders/service.go", 50)


def test_file_over_the_size_cap_none(tmp_path, monkeypatch):
    from langgraph_postgres_starter.agents.incident_triage.codeintel import extract

    (tmp_path / "big.py").write_text("def f():\n    return 1\n")
    monkeypatch.setattr(extract, "MAX_FILE_BYTES", 10)
    assert enclosing_definition(tmp_path, "big.py", 1) is None


def test_invalid_utf8_is_replaced(tmp_path):
    (tmp_path / "bad.py").write_bytes(b"def f():\n    return '\xff'\n")
    d = enclosing_definition(tmp_path, "bad.py", 2)
    assert d is not None
    assert (d.symbol, d.start_line, d.end_line) == ("f", 1, 2)
    assert d.text == "def f():\n    return '�'"


def test_tsx_definition(tmp_path):
    (tmp_path / "Cart.tsx").write_text(
        textwrap.dedent(
            """\
            export function Cart({ items }: { items: string[] }) {
              return (
                <ul>
                  {items.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              );
            }
            """
        )
    )
    # Line 5 is in an arrow function, which has no name: Cart encloses it.
    d = enclosing_definition(tmp_path, "Cart.tsx", 5)
    assert d is not None
    assert (d.symbol, d.start_line, d.end_line, d.language) == ("Cart", 1, 9, "tsx")


@pytest.mark.parametrize(
    ("name", "source", "expected"),
    [
        (
            "grid.cpp",
            """\
            int& a::b::Grid::cell(int i) {
              return cells_[i];
            }
            a::b::Grid::~Grid() {
              delete[] cells_;
            }
            bool a::b::Grid::operator==(const Grid& other) const {
              return size_ == other.size_;
            }
            """,
            [(2, "cell", 1, 3), (5, "~Grid", 4, 6), (8, "operator==", 7, 9)],
        ),
        (
            "invoice.rb",
            """\
            module Shop
              class Billing::Invoice
                def self.build(order)
                  new(order)
                end
              end
            end
            """,
            [(4, "build", 3, 5), (2, "Invoice", 2, 6), (7, "Shop", 1, 7)],
        ),
        (
            "Strings.kt",
            """\
            fun <T> List<T>.second(): T {
                return this[1]
            }
            """,
            [(2, "second", 1, 3)],
        ),
        (
            "Rates.scala",
            """\
            object Rates {
              def double(x: Int): Int =
                x * 2

              def half(x: Int): Int = x / 2
            }
            """,
            [(3, "double", 2, 3), (4, "Rates", 1, 6), (5, "half", 5, 5)],
        ),
    ],
)
def test_bare_symbols(tmp_path, name, source, expected):
    (tmp_path / name).write_text(textwrap.dedent(source))
    for at, symbol, start, end in expected:
        d = enclosing_definition(tmp_path, name, at)
        assert d is not None, at
        assert (d.symbol, d.start_line, d.end_line) == (symbol, start, end), at


@pytest.mark.parametrize(
    ("path", "grammar"),
    [
        ("a.js", "javascript"),
        ("a.mjs", "javascript"),
        ("a.cjs", "javascript"),
        ("a.jsx", "javascript"),
        ("a.ts", "typescript"),
        ("a.tsx", "tsx"),
        ("A.java", "java"),
        ("A.kt", "kotlin"),
        ("build.gradle.kts", "kotlin"),
        ("A.scala", "scala"),
        ("a.sc", "scala"),
        ("a.py", "python"),
        ("a.go", "go"),
        ("a.php", "php"),
        ("a.rb", "ruby"),
        ("a.c", "c"),
        ("a.h", "c"),
        ("a.cpp", "cpp"),
        ("a.cc", "cpp"),
        ("a.cxx", "cpp"),
        ("a.hpp", "cpp"),
        ("a.hh", "cpp"),
    ],
)
def test_language_for(path, grammar):
    spec = language_for(f"src/{path}")
    assert spec is not None and spec.grammar == grammar
    assert spec is LANGUAGES[grammar]


def test_languages_are_the_twelve_grammars():
    assert set(LANGUAGES) == {
        "javascript",
        "typescript",
        "tsx",
        "java",
        "kotlin",
        "scala",
        "python",
        "go",
        "php",
        "ruby",
        "c",
        "cpp",
    }
    assert all(name == spec.grammar for name, spec in LANGUAGES.items())


@pytest.mark.parametrize("grammar", sorted(LANGUAGES))
def test_definition_types_exist_in_the_grammar(grammar):
    language = languages.parse(grammar, b"", grammar_dir=None).language
    for node_type in LANGUAGES[grammar].definition_types:
        assert language.id_for_node_kind(node_type, True) is not None, node_type


def test_parse_rejects_other_grammars():
    with pytest.raises(ValueError, match="rust"):
        languages.parse("rust", b"fn main() {}", grammar_dir=None)


def test_missing_grammar_raises_and_never_downloads(tmp_path, monkeypatch):
    real = {name: getattr(tree_sitter_language_pack, name) for name in _LOADERS}
    for module in (tree_sitter_language_pack, tree_sitter_language_pack._native):
        for name in (*_DOWNLOADERS, *_LOADERS):
            monkeypatch.setattr(module, name, _no_download)
    monkeypatch.setenv("APP_GRAMMAR_DIR", str(tmp_path))
    get_settings.cache_clear()
    with pytest.raises(GrammarMissingError) as error:
        enclosing_definition(_app("python"), "orders/service.py", 24)
    message = str(error.value)
    assert "'python'" in message and str(tmp_path) in message
    assert languages.FETCH_COMMAND in message
    assert list(tmp_path.iterdir()) == []
    # Back on the library's default folder, which has the grammars: still no download.
    for name, function in real.items():
        monkeypatch.setattr(tree_sitter_language_pack, name, function)
    monkeypatch.delenv("APP_GRAMMAR_DIR")
    get_settings.cache_clear()
    d = enclosing_definition(_app("python"), "orders/service.py", 24)
    assert d is not None and d.symbol == "_flatten_customer"


def test_download_fetches_exactly_the_grammars(tmp_path, monkeypatch, capsys):
    requested: list[list[str]] = []

    def download(names: list[str]) -> int:
        requested.append(names)
        return len(names)

    monkeypatch.setattr(tree_sitter_language_pack, "download", download)
    monkeypatch.setattr(tree_sitter_language_pack, "downloaded_languages", lambda: list(LANGUAGES))
    monkeypatch.setenv("APP_GRAMMAR_DIR", str(tmp_path))
    assert languages.main(["--download"]) == 0
    assert requested == [list(LANGUAGES)]
    assert tree_sitter_language_pack.cache_dir().startswith(str(tmp_path))
    assert str(tmp_path) in capsys.readouterr().out


def test_download_needs_the_flag():
    with pytest.raises(SystemExit):
        languages.main([])
