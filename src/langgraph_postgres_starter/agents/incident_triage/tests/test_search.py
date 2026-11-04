"""Log literals and the ripgrep search for them (spec §9.6 item 5).

``literals_from_logs`` takes the constant text of error and log lines; the table shows
which lines count and what each rule removes. ``search_literals`` runs ``rg`` on the
fixture apps and on small folders made by the tests: the python fixture's raise site,
a literal that starts with ``-``, a symbolic link, the match cap, and ``rg`` missing
from ``PATH``. A fake ``rg`` tests the caps on events and output, and the timeout.
"""

import json
import sys
from pathlib import Path

import pytest

from langgraph_postgres_starter.agents.incident_triage.codeintel import search
from langgraph_postgres_starter.agents.incident_triage.codeintel.search import (
    RipgrepMissing,
    SearchMatch,
    literals_from_logs,
    search_literals,
)
from langgraph_postgres_starter.redaction import REDACTED
from langgraph_postgres_starter.settings import get_settings


def _app(name: str) -> Path:
    return get_settings().fixtures_dir / "apps" / name


# The previous log of the c-segfault fixture: no stack trace, only log messages.
_C_SEGFAULT_LOG = """\
2026-11-02T13:05:41Z INFO order-batcher started, spool /var/spool/orders/next.batch
2026-11-02T13:05:46Z INFO processing order batch for warehouse 1
2026-11-02T13:05:46Z INFO batch done for warehouse LHR1: 42 orders, 377 units
2026-11-02T13:05:52Z INFO processing order batch for warehouse 2
2026-11-02T13:05:52Z INFO batch done for warehouse MAN2: 18 orders, 140 units
2026-11-02T13:05:58Z INFO processing order batch for warehouse 4
Segmentation fault (core dumped)
"""


@pytest.mark.parametrize(
    ("log", "expected"),
    [
        # The message after the log level, without its number. The three lines give
        # the same literal, once.
        (_C_SEGFAULT_LOG, ["processing order batch for warehouse"]),
        # "processing" counts only after a log level.
        ("processing order batch for warehouse 4", []),
        # A line with no marker.
        ("2026-11-02T13:05:46Z INFO batch done for warehouse LHR1: 42 orders", []),
        ("panic: assignment to entry in nil map", ["panic: assignment to entry in nil map"]),
        # A quoted string is removed; the ends of a piece lose spaces and punctuation.
        (
            "TypeError: Cannot read properties of undefined (reading 'map')",
            ["TypeError: Cannot read properties of undefined (reading"],
        ),
        (
            'Exception in thread "main" java.lang.IllegalStateException: payout po_20398 '
            "is RETRY_SCHEDULED, expected APPROVED",
            ["java.lang.IllegalStateException: payout po"],
        ),
        # The only piece is shorter than 12 characters.
        ("KeyError: 'customer'", []),
        ("panic: bad pointer 0xdeadbeef in handler", ["panic: bad pointer"]),
        (
            "Error: lookup failed for deadbeef-dead-beef-dead-beefdeadbeef in cache",
            ["Error: lookup failed for"],
        ),
        ("Error: no route to host fe80::1ff:fe23:4567:890a upstream", ["Error: no route to host"]),
        (
            "Error: connect ECONNREFUSED 10.20.30.40:5432 while dialing",
            ["Error: connect ECONNREFUSED"],
        ),
        # A single digit inside a word stays; a run of digits never does.
        ("Error: utf8 decoder rejected byte 255", ["Error: utf8 decoder rejected byte"]),
        # FATAL is a marker and a log level.
        (
            "2026-11-02T10:00:00Z FATAL could not load the config file",
            ["could not load the config file"],
        ),
        (
            "time=2026-11-02T10:00:00Z level=error msg=reserve error=nil map assignment",
            ["msg=reserve error=nil map assignment"],
        ),
        # Redaction runs first; a redacted value splits the line like a variable part.
        (
            "Error: login failed password=hunter2secret for the admin account",
            ["Error: login failed password"],
        ),
        # A C0 control character, as in binary junk, splits a line like a variable part;
        # a tab does not.
        (
            "Error: decoder rejected input abc\x00defghijklmnop qrstuv",
            ["Error: decoder rejected input abc"],
        ),
        ("Error: bad frame\x01\x1f header checksum mismatch", ["header checksum mismatch"]),
        ("panic: worker\tlost its lease", ["panic: worker\tlost its lease"]),
        # Newest first: the line nearest the crash leads.
        (
            "2026-11-02T10:00:00Z INFO processing refunds for merchant 7\n"
            "panic: assignment to entry in nil map",
            ["panic: assignment to entry in nil map", "processing refunds for merchant"],
        ),
        ("", []),
    ],
)
def test_literals_table(log, expected):
    literals = literals_from_logs(log)
    assert literals == expected
    assert all(REDACTED not in literal and "hunter2" not in literal for literal in literals)
    assert not [c for literal in literals for c in literal if c < " " and c != "\t"]


def test_literals_capped_newest_first():
    log = "\n".join(f"panic: worker {name} lost its lease" for name in "abcdef")
    assert literals_from_logs(log, max_literals=2) == [
        "panic: worker f lost its lease",
        "panic: worker e lost its lease",
    ]
    assert literals_from_logs(log, max_literals=0) == []


async def test_search_finds_raise_site():
    root = _app("python")
    # rg is not given an empty literal, one with a line break or one with a NUL byte
    # (which no process argument can hold).
    literals = ['data["customer"]', "no fixture app has this text", "", "two\nlines", "a\x00b"]
    matches = await search_literals(root, literals)
    assert matches == [SearchMatch(path="orders/service.py", line=24, literal='data["customer"]')]


async def test_search_stops_at_max_matches():
    matches = await search_literals(_app("python"), ["import"], max_matches=2)
    assert len(matches) == 2
    assert all(match.literal == "import" for match in matches)


async def test_literal_starting_with_dash_is_a_pattern(tmp_path):
    # As an option, --files would make rg list the files instead of searching.
    (tmp_path / "cli.py").write_text('USAGE = "--files are listed here"\n')
    matches = await search_literals(tmp_path, ["--files are listed"])
    assert matches == [SearchMatch(path="cli.py", line=1, literal="--files are listed")]


async def test_search_does_not_follow_symlinks(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.py").write_text("processing order batch for warehouse\n")
    root = tmp_path / "repo"
    root.mkdir()
    (root / "linked").symlink_to(outside)
    (root / "linked.py").symlink_to(outside / "secret.py")
    assert await search_literals(root, ["processing order batch for warehouse"]) == []


async def test_ripgrep_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(RipgrepMissing, match="install ripgrep"):
        await search_literals(_app("python"), ['data["customer"]'])


def _match_event(root: Path, line: int) -> str:
    """rg's JSON event for a match of ``LITERAL`` on line ``line`` of ``root/a.py``."""
    return json.dumps(
        {
            "type": "match",
            "data": {
                "path": {"text": str(root / "a.py")},
                "line_number": line,
                "submatches": [{"match": {"text": LITERAL}, "start": 0, "end": len(LITERAL)}],
            },
        }
    )


LITERAL = "the literal text"


def _fake_rg(folder: Path, monkeypatch: pytest.MonkeyPatch, script: str) -> None:
    """Make ``rg`` a Python ``script`` in ``folder``, the only folder on ``PATH``.

    Coverage does not measure it: it is no code under test, and one that is killed
    would leave an empty data file, which fails the coverage report.
    """
    rg = folder / "rg"
    rg.write_text(f"#!{sys.executable}\nimport sys, time\n{script}")
    rg.chmod(0o755)
    monkeypatch.setenv("PATH", str(folder))
    monkeypatch.delenv("COVERAGE_PROCESS_CONFIG", raising=False)


async def test_search_skips_long_events(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    # An event longer than MAX_EVENT_BYTES, sent in two parts: its rest looks like a
    # match (line 99) and must not count. Then a line that is not JSON, then a match.
    _fake_rg(
        tmp_path,
        monkeypatch,
        f"sys.stdout.write('x' * 100); sys.stdout.flush(); time.sleep(0.3)\n"
        f"print({_match_event(root, 99)!r})\n"
        f"print('not json')\n"
        f"print({_match_event(root, 3)!r})\n",
    )
    monkeypatch.setattr(search, "MAX_EVENT_BYTES", 64)

    assert await search_literals(root, [LITERAL]) == [SearchMatch("a.py", 3, LITERAL)]


async def test_search_stops_at_its_timeout(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    script = f"print({_match_event(root, 3)!r}, flush=True)\ntime.sleep(60)\n"
    _fake_rg(tmp_path, monkeypatch, script)
    monkeypatch.setattr(search, "SEARCH_TIMEOUT_S", 1.0)

    matches = await search_literals(root, [LITERAL])

    # It returns what it found, and stopped rg: else the wait for rg would take 60 s.
    assert matches == [SearchMatch("a.py", 3, LITERAL)]


async def test_search_reads_at_most_max_output_bytes(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    first = _match_event(root, 1)
    _fake_rg(tmp_path, monkeypatch, f"print({first!r})\nprint({_match_event(root, 2)!r})\n")
    monkeypatch.setattr(search, "MAX_OUTPUT_BYTES", len(first) + 1)

    assert await search_literals(root, [LITERAL]) == [SearchMatch("a.py", 1, LITERAL)]
