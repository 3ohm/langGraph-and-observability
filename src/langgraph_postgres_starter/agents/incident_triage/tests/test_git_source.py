"""Checking out a commit with the git CLI into a size-bounded cache (spec §9.6 item 1).

The remote is a local bare repository with two commits, reached over ``file://``. It
sets ``uploadpack.allowReachableSHA1InWant``: git's protocol v2, the default, already
serves a commit by SHA, and the setting keeps that true under protocol v0.
"""

import asyncio
import hashlib
import os
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
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import SourceRef

TOKEN = "ghp_" + "T0k3n" * 8


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
    return GitSourceProvider(cache, secret, max_cache_mb, allow_file_urls=True, **kwargs)


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
        git_vars = {k: v for k, v in spawn.env.items() if k.startswith("GIT_")}
        assert git_vars == {
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ALLOW_PROTOCOL": "https:file",
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "http.extraHeader",
            "GIT_CONFIG_VALUE_0": f"Authorization: Bearer {TOKEN}",
        }
    assert TOKEN not in str(exc_info.value)
    assert TOKEN not in repr(logs)
    assert TOKEN not in (path / ".git" / "config").read_text()


async def test_git_without_token_sets_no_header(tmp_path, remote, spawns):
    await _provider(tmp_path / "cache").checkout(_ref(remote, remote.second))

    assert spawns
    for spawn in spawns:
        assert "GIT_CONFIG_COUNT" not in spawn.env
        assert spawn.env["GIT_TERMINAL_PROMPT"] == "0"


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


async def test_cache_eviction_respects_budget(tmp_path, remote):
    cache = tmp_path / "cache"
    now = time.time()
    # Two older checkouts of another repository, 600 KiB each, and a folder that is not
    # a checkout. The budget is 1 MiB.
    other = cache / ("0" * 16)
    for name, age in (("1" * 40, 300), ("2" * 40, 100)):
        entry = other / name
        entry.mkdir(parents=True)
        (entry / "blob.bin").write_bytes(b"x" * 600 * 1024)
        os.utime(entry, (now - age, now - age))
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
    assert [log["path"] for log in evicted] == [str(other / ("1" * 40))]

    # With no budget, every other checkout goes, but never the one just checked out, and
    # a repository folder that is left empty goes too.
    second = await _provider(cache, max_cache_mb=0).checkout(_ref(remote, remote.second))

    assert not other.exists()
    assert [p.name for p in _repo_dir(cache, remote).iterdir()] == [remote.second]
    assert (second / "app.txt").read_text() == "second\n"
    assert (unrelated / "big.bin").exists()


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
    provider = GitSourceProvider(tmp_path / "cache", None, 100)

    with pytest.raises(ValueError, match=error) as exc_info:
        await provider.checkout(SourceRef(repo_url=repo_url, commit=commit, origin="annotation"))

    assert spawns == []
    assert "s3cr3t" not in str(exc_info.value)
