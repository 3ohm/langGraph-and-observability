"""Checking out a commit with the git CLI into a size-bounded cache (spec §9.6 item 1).

The remote is a local bare repository with two commits, reached over ``file://``. It
sets ``uploadpack.allowReachableSHA1InWant``: git's protocol v2, the default, already
serves a commit by SHA, and the setting keeps that true under protocol v0.

The https:// tests never reach a network: ``spawns_failing`` makes every git process
fail at once, and the tests read the argv and environment it was given.
"""

import asyncio
import hashlib
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr
from structlog.testing import capture_logs

from langgraph_postgres_starter.agents.incident_triage.adapters.source.git import (
    GitError,
    GitSourceProvider,
    check_repo_url,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import SourceRef

TOKEN = "ghp_" + "T0k3n" * 8
ALLOWED = ("github.com", "Git.Example.com.")


@dataclass(frozen=True)
class Remote:
    url: str
    first: str
    second: str


def _git(cwd: Path, *args: str) -> str:
    # The developer's git config must not change the test repository.
    env = os.environ | {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    return subprocess.run(  # noqa: S603 - fixed git commands on a test folder
        ["git", *args],  # noqa: S607 - git from PATH, as the provider runs it
        cwd=cwd,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture(scope="module")
def remote(tmp_path_factory) -> Remote:
    """A bare repository whose ``main`` has two commits; ``app.txt`` changes between them."""
    root = tmp_path_factory.mktemp("remote")
    bare, work = root / "orders.git", root / "work"
    work.mkdir()
    _git(root, "init", "-q", "--bare", "-b", "main", str(bare))
    _git(bare, "config", "uploadpack.allowReachableSHA1InWant", "true")
    _git(work, "init", "-q", "-b", "main")
    shas = []
    for content in ("first", "second"):
        (work / "app.txt").write_text(f"{content}\n")
        _git(work, "add", "app.txt")
        _git(work, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", content)
        shas.append(_git(work, "rev-parse", "HEAD"))
    _git(work, "push", "-q", str(bare), "main")
    return Remote(url=bare.as_uri(), first=shas[0], second=shas[1])


def _ref(remote: Remote, commit: str) -> SourceRef:
    return SourceRef(repo_url=remote.url, commit=commit, origin="annotation")


def _provider(cache: Path, token: str | None = None, max_cache_mb: int = 100, **kwargs: Any):
    secret = None if token is None else SecretStr(token)
    return GitSourceProvider(
        cache, secret, max_cache_mb, allowed_hosts=ALLOWED, allow_file_urls=True, **kwargs
    )


def _repo_dir(cache: Path, remote: Remote) -> Path:
    return cache / hashlib.sha256(remote.url.encode()).hexdigest()[:16]


@dataclass
class Spawn:
    argv: tuple[str, ...]
    env: dict[str, str]
    process: asyncio.subprocess.Process | None = None


@pytest.fixture
def spawns(monkeypatch) -> list[Spawn]:
    """Every process that the provider starts: its real argv and environment."""
    spawned: list[Spawn] = []
    real_exec = asyncio.create_subprocess_exec

    async def recording_exec(program, *args, **kwargs):
        spawn = Spawn(argv=(program, *args), env=dict(kwargs["env"]))
        spawned.append(spawn)
        spawn.process = await real_exec(program, *args, **kwargs)
        return spawn.process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", recording_exec)
    return spawned


@pytest.fixture
def spawns_failing(monkeypatch) -> list[Spawn]:
    """Like ``spawns``, but each process is one that exits 1 at once, never git."""
    spawned: list[Spawn] = []
    real_exec = asyncio.create_subprocess_exec

    async def failing_exec(program, *args, **kwargs):
        spawn = Spawn(argv=(program, *args), env=dict(kwargs["env"]))
        spawned.append(spawn)
        spawn.process = await real_exec(sys.executable, "-c", "raise SystemExit(1)", **kwargs)
        return spawn.process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", failing_exec)
    return spawned


def _git_vars(spawn: Spawn) -> dict[str, str]:
    return {k: v for k, v in spawn.env.items() if k.startswith("GIT_")}


def _leftovers(repo_dir: Path) -> list[str]:
    """The temporary folders left in a repository's cache folder."""
    return [p.name for p in repo_dir.iterdir() if p.name.startswith(".")]


async def test_git_checkout_by_sha_uses_cache(tmp_path, remote, spawns):
    cache = tmp_path / "cache"

    path = await _provider(cache).checkout(_ref(remote, remote.first))

    assert path == _repo_dir(cache, remote) / remote.first
    assert (path / "app.txt").read_text() == "first\n"
    assert [s.argv[1] for s in spawns] == ["init", "remote", "fetch", "rev-parse", "checkout"]
    assert _leftovers(_repo_dir(cache, remote)) == []

    # A second run, with its own provider, reuses the checkout and touches its mtime.
    spawns.clear()
    os.utime(path, (1_000_000, 1_000_000))
    with capture_logs() as logs:
        again = await _provider(cache).checkout(_ref(remote, remote.first.upper()))

    assert again == path
    assert spawns == []
    assert path.stat().st_mtime > time.time() - 60
    assert [log["event"] for log in logs] == ["source.checkout.cached"]


async def test_git_token_not_in_argv(tmp_path, remote, spawns, monkeypatch):
    # The parent's git variables are not passed on: this GIT_DIR would break the checkout.
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "elsewhere"))
    monkeypatch.setenv("GIT_CONFIG_PARAMETERS", "'core.bare'='true'")
    cache = tmp_path / "cache"

    with capture_logs() as logs:
        path = await _provider(cache, TOKEN).checkout(_ref(remote, remote.first))
        with pytest.raises(GitError) as exc_info:
            await _provider(cache, TOKEN).checkout(_ref(remote, "0" * 40))

    assert (path / "app.txt").read_text() == "first\n"
    assert spawns
    for spawn in spawns:
        assert spawn.argv[0] == "git"
        assert not any(TOKEN in arg for arg in spawn.argv), spawn.argv
        assert "-c" not in spawn.argv
        assert not any("extraHeader" in arg for arg in spawn.argv)
        # A file:// URL has no host to scope the header to, so it gets none.
        assert _git_vars(spawn) == {
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ALLOW_PROTOCOL": "file",
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "http.followRedirects",
            "GIT_CONFIG_VALUE_0": "false",
        }
    assert TOKEN not in str(exc_info.value)
    assert TOKEN not in repr(logs)
    assert TOKEN not in (path / ".git" / "config").read_text()


@pytest.mark.parametrize(
    ("repo_url", "host"),
    [
        ("https://github.com/example/orders", "github.com"),
        # The host is matched lower-cased and without a trailing dot, on either side.
        ("https://GIT.example.COM./example/orders.git", "git.example.com"),
    ],
)
async def test_git_token_header_is_scoped_to_the_repository_host(
    tmp_path, spawns_failing, monkeypatch, repo_url, host
):
    monkeypatch.setenv("GIT_ASKPASS", "/bin/echo")
    provider = _provider(tmp_path / "cache", TOKEN)

    with pytest.raises(GitError, match="git init failed") as exc_info:
        await provider.checkout(SourceRef(repo_url=repo_url, commit="main", origin="annotation"))

    [spawn] = spawns_failing
    assert _git_vars(spawn) == {
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "https",
        "GIT_CONFIG_COUNT": "2",
        "GIT_CONFIG_KEY_0": f"http.https://{host}/.extraHeader",
        "GIT_CONFIG_VALUE_0": f"Authorization: Bearer {TOKEN}",
        "GIT_CONFIG_KEY_1": "http.followRedirects",
        "GIT_CONFIG_VALUE_1": "false",
    }
    assert not any(TOKEN in arg for arg in spawn.argv)
    assert TOKEN not in str(exc_info.value)


async def test_git_without_token_sets_no_header_but_refuses_redirects(tmp_path, spawns_failing):
    provider = _provider(tmp_path / "cache")

    with pytest.raises(GitError):
        await provider.checkout(_ref(Remote("https://github.com/example/orders", "", ""), "main"))

    [spawn] = spawns_failing
    assert _git_vars(spawn) == {
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "https",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "http.followRedirects",
        "GIT_CONFIG_VALUE_0": "false",
    }


async def test_git_remote_and_cache_use_the_canonical_url(tmp_path, spawns_failing):
    cache = tmp_path / "cache"
    provider = _provider(cache)

    with pytest.raises(GitError):
        await provider.checkout(
            SourceRef(
                repo_url="https://GitHub.com.:443/example/orders", commit="main", origin="config"
            )
        )

    canonical = "https://github.com/example/orders"
    assert check_repo_url(canonical, allowed_hosts=ALLOWED) == canonical
    assert (
        check_repo_url("https://GitHub.com.:443/example/orders", allowed_hosts=ALLOWED) == canonical
    )
    [spawn] = spawns_failing
    assert spawn.argv[1] == "init"
    assert (cache / hashlib.sha256(canonical.encode()).hexdigest()[:16]).is_dir()


async def test_git_checkout_by_branch_keys_the_cache_by_sha(tmp_path, remote, spawns):
    cache = tmp_path / "cache"
    provider = _provider(cache)

    path = await provider.checkout(_ref(remote, "main"))
    # A branch may move, so every checkout of a branch fetches it again.
    again = await provider.checkout(_ref(remote, "main"))
    by_sha = await provider.checkout(_ref(remote, remote.second))

    assert path == again == by_sha == _repo_dir(cache, remote) / remote.second
    assert (path / "app.txt").read_text() == "second\n"
    fetches = [s.argv for s in spawns if s.argv[1] == "fetch"]
    assert [argv[-2:] for argv in fetches] == [("origin", "main")] * 2
    assert all(argv[-3] == "--" for argv in fetches)
    assert _leftovers(_repo_dir(cache, remote)) == []


async def test_concurrent_checkouts_never_see_a_partial_tree(tmp_path, remote):
    cache = tmp_path / "cache"

    paths = await asyncio.gather(
        *(_provider(cache).checkout(_ref(remote, remote.first)) for _ in range(3))
    )

    assert len(set(paths)) == 1
    assert (paths[0] / "app.txt").read_text() == "first\n"
    assert [p.name for p in _repo_dir(cache, remote).iterdir()] == [remote.first]


def _fake_entry(repo_dir: Path, name: str, age_s: float, kib: int = 600) -> Path:
    """A checkout of ``kib`` KiB in ``repo_dir``, last used ``age_s`` seconds ago."""
    entry = repo_dir / name
    entry.mkdir(parents=True)
    (entry / "blob.bin").write_bytes(b"x" * kib * 1024)
    _age(entry, age_s)
    return entry


def _age(entry: Path, age_s: float) -> None:
    then = time.time() - age_s
    os.utime(entry, (then, then))


async def test_cache_eviction_respects_budget(tmp_path, remote):
    cache = tmp_path / "cache"
    # Two checkouts of another repository, 600 KiB each, last used well over the
    # default minimum age (900 s) ago, and a folder that is not a checkout. The budget
    # is 1 MiB.
    other = cache / ("0" * 16)
    oldest = _fake_entry(other, "1" * 40, age_s=3000)
    _fake_entry(other, "2" * 40, age_s=2000)
    unrelated = cache / "notes"
    unrelated.mkdir()
    (unrelated / "big.bin").write_bytes(b"x" * 2 * 1024 * 1024)

    with capture_logs() as logs:
        path = await _provider(cache, max_cache_mb=1).checkout(_ref(remote, remote.first))

    # The oldest checkout goes; then the cache is within budget.
    assert sorted(p.name for p in other.iterdir()) == ["2" * 40]
    assert path.is_dir()
    assert (unrelated / "big.bin").exists()
    evicted = [log for log in logs if log["event"] == "source.cache.evicted"]
    assert [log["path"] for log in evicted] == [str(oldest)]
    assert [log["event"] for log in logs if "over_budget" in log["event"]] == []

    # With no budget, every other checkout old enough goes, but never the one just
    # checked out, and a repository folder that is left empty goes too.
    _age(path, 1000)
    second = await _provider(cache, max_cache_mb=0).checkout(_ref(remote, remote.second))

    assert not other.exists()
    assert [p.name for p in _repo_dir(cache, remote).iterdir()] == [remote.second]
    assert (second / "app.txt").read_text() == "second\n"
    assert (unrelated / "big.bin").exists()


async def test_eviction_spares_entries_a_run_may_still_read(tmp_path, remote):
    cache = tmp_path / "cache"
    other = cache / ("0" * 16)
    old = _fake_entry(other, "1" * 40, age_s=3000)
    # Used by runs 60 s and 800 s ago: within the minimum age, so a run may still read them.
    recent = [_fake_entry(other, "2" * 40, age_s=60), _fake_entry(other, "3" * 40, age_s=800)]

    with capture_logs() as logs:
        await _provider(cache, max_cache_mb=1).checkout(_ref(remote, remote.first))

    assert not old.exists()
    assert all(entry.is_dir() for entry in recent)
    # Still over budget: one line with the sizes, and no path of another repository.
    [over] = [log for log in logs if log["event"] == "source.cache_over_budget"]
    assert over["max_bytes"] == 1024 * 1024
    assert over["bytes"] > 2 * 600 * 1024
    assert "0" * 16 not in repr(over)

    # A shorter minimum age lets the eviction take them, oldest first.
    with capture_logs() as logs:
        await _provider(cache, max_cache_mb=1, min_age_s=30).checkout(_ref(remote, remote.second))

    assert [entry.is_dir() for entry in recent] == [True, False]
    assert [log["event"] for log in logs if "over_budget" in log["event"]] == []


async def test_reuse_of_an_entry_that_vanishes_fetches_again(tmp_path, remote, spawns, monkeypatch):
    cache = tmp_path / "cache"
    path = await _provider(cache).checkout(_ref(remote, remote.first))
    spawns.clear()
    real_utime = os.utime
    vanished: list[Path] = []

    def utime_after_eviction(target, *args, **kwargs):
        # Another run's eviction deletes the entry between the check and the touch.
        if Path(target) == path and not vanished:
            vanished.append(path)
            shutil.rmtree(path)
        return real_utime(target, *args, **kwargs)

    monkeypatch.setattr(os, "utime", utime_after_eviction)

    again = await _provider(cache).checkout(_ref(remote, remote.first))

    assert vanished == [path]
    assert again == path
    assert (again / "app.txt").read_text() == "first\n"
    assert [s.argv[1] for s in spawns] == ["init", "remote", "fetch", "rev-parse", "checkout"]


async def test_failed_fetch_removes_its_temp_dir(tmp_path, remote):
    cache = tmp_path / "cache"

    with pytest.raises(GitError, match="git fetch failed"):
        await _provider(cache).checkout(_ref(remote, "0" * 40))
    with pytest.raises(GitError, match="git fetch failed"):
        await _provider(cache).checkout(_ref(remote, "no-such-branch"))

    assert _leftovers(_repo_dir(cache, remote)) == []
    assert not (_repo_dir(cache, remote) / ("0" * 40)).exists()


async def test_git_timeout_kills_git_and_removes_its_temp_dir(tmp_path, remote, monkeypatch):
    real_exec = asyncio.create_subprocess_exec
    started: list[asyncio.subprocess.Process] = []

    async def hanging_exec(program, *args, **kwargs):
        sleeper = [sys.executable, "-c", "import time; time.sleep(60)"]
        process = await real_exec(*sleeper, **kwargs)
        started.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", hanging_exec)
    cache = tmp_path / "cache"

    with pytest.raises(GitError, match="git init timed out after 0.2 s"):
        await _provider(cache, timeout_s=0.2).checkout(_ref(remote, remote.first))

    assert len(started) == 1 and started[0].returncode is not None
    assert _leftovers(_repo_dir(cache, remote)) == []


@pytest.mark.parametrize(
    ("repo_url", "commit", "error"),
    [
        ("-uhttps://example.com/orders", "main", "https://"),
        ("http://github.com/example/orders", "main", "https://"),
        ("ssh://git@github.com/example/orders", "main", "https://"),
        ("git@github.com:example/orders.git", "main", "https://"),
        ("ext::sh -c touch% /tmp/pwned", "main", "https://"),
        ("https://bot:s3cr3t@github.com/example/orders", "main", "credentials"),
        ("https://github.com/example/orders repo", "main", "whitespace"),
        ("https:///example/orders", "main", "host"),
        ("https://gitlab.example.com/example/orders", "main", "'gitlab.example.com' is not in"),
        ("https://github.com.evil.example/example/orders", "main", "is not in"),
        ("https://169.254.169.254/latest/meta-data", "main", "'169.254.169.254' is not in"),
        ("https://github.com:8443/example/orders", "main", "port"),
        ("https://github.com:x/example/orders", "main", "port"),
        ("https://github.com/example/orders?x=1", "main", "query"),
        ("https://github.com", "main", "path"),
        ("file:///srv/git/orders.git", "main", "https://"),
        ("https://github.com/example/orders", "--upload-pack=touch /tmp/pwned", "commit"),
        ("https://github.com/example/orders", "main:refs/heads/x", "commit"),
        ("https://github.com/example/orders", "+main", "commit"),
        ("https://github.com/example/orders", "feature/../main", "commit"),
        ("https://github.com/example/orders", "main.lock", "commit"),
        ("https://github.com/example/orders", "release/", "commit"),
        ("https://github.com/example/orders", "@{-1}", "commit"),
        ("https://github.com/example/orders", "", "commit"),
        ("https://github.com/example/orders", "abc 123", "commit"),
    ],
)
async def test_invalid_sources_are_rejected_before_git_runs(
    tmp_path, spawns, repo_url, commit, error
):
    # Production providers serve https:// only; file:// is for tests.
    provider = GitSourceProvider(
        tmp_path / "cache", SecretStr(TOKEN), 100, allowed_hosts=["github.com"]
    )

    with pytest.raises(ValueError, match=error) as exc_info:
        await provider.checkout(SourceRef(repo_url=repo_url, commit=commit, origin="annotation"))

    assert spawns == []
    assert "s3cr3t" not in str(exc_info.value)
    assert TOKEN not in str(exc_info.value)


def test_file_urls_need_the_test_flag(tmp_path):
    assert check_repo_url("file:///srv/git/orders.git", allowed_hosts=[], allow_file=True)
    with pytest.raises(ValueError, match="https://"):
        check_repo_url("file:///srv/git/orders.git", allowed_hosts=["github.com"])
    with pytest.raises(ValueError, match="host"):
        check_repo_url("file://fileserver/srv/git/orders.git", allowed_hosts=[], allow_file=True)
