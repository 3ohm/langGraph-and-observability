"""Check out a repository at a commit with the git CLI, into a cache (spec §9.6 item 1).

``GitSourceProvider.checkout`` runs ``git init``, ``git remote add``, ``git fetch
--depth 1`` of the one commit and ``git checkout --detach FETCH_HEAD``. The cache entry
is ``<cache_dir>/<sha256(repo_url)[:16]>/<sha>``, keyed by the commit's full 40-hex
SHA. A ref that is not a full SHA, such as a branch, is fetched on every call, because
it can move; its entry is named by the SHA that ``FETCH_HEAD`` resolves to.

Atomic entries: each checkout is made in a temporary folder next to its entry and
then renamed into place, so another run never sees a half-written tree. When two runs
check out the same commit at once, the first rename wins and the second run uses its
entry. A failed checkout deletes its temporary folder.

Eviction: after each new checkout, while the cache is over ``max_cache_mb``, the entry
with the oldest modification time is deleted, never the one just checked out. A
checkout that is reused gets a new modification time, so the order is least recently
used. Only folders with the names above count: other files in ``cache_dir`` are left
alone.

Untrusted input: a ref's URL and commit come from the cluster (annotations, image
labels, the source map), so ``check_repo_url`` and ``check_commit`` validate them
before any git process starts, and ``--`` comes before git's positional arguments. A
URL must be ``https://`` (``file://`` only with ``allow_file_urls``, for tests), with a
host and without credentials.

The token: git gets it through the ``GIT_CONFIG_COUNT``, ``GIT_CONFIG_KEY_0`` and
``GIT_CONFIG_VALUE_0`` variables as ``http.extraHeader: Authorization: Bearer
<token>``, so it is never in an argument list that ``ps`` shows, never in the URL,
never in ``.git/config`` and never logged. GitHub accepts a token as a Bearer header
for git over HTTPS; GitLab's git HTTP endpoint ignores Bearer and needs Basic auth
(checked 2026-11-04), so a GitLab token does not work this way.

Each git process gets ``GIT_TERMINAL_PROMPT=0`` (fail rather than prompt), a
``GIT_ALLOW_PROTOCOL`` of the allowed schemes, and none of the parent's other
``GIT_*`` variables. Each git call is stopped after ``timeout_s``: its process group is
killed and the call raises ``GitError``.
"""

import asyncio
import hashlib
import os
import re
import shutil
import signal
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import SecretStr

from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import (
    SourceProvider,
    SourceRef,
)
from langgraph_postgres_starter.logging import get_logger
from langgraph_postgres_starter.redaction import REDACTED, redact

_log = get_logger(__name__)

# The longest that one git call may run, in seconds.
DEFAULT_GIT_TIMEOUT_S = 120.0
# The most characters of git's error output that a GitError message keeps.
_MAX_ERROR_CHARS = 1000
_MAX_URL_CHARS = 2048

_FULL_SHA = re.compile(r"[0-9a-f]{40}")
_FULL_SHA_ANY_CASE = re.compile(r"[0-9a-fA-F]{40}")
# A branch or tag name: no spaces, no ~^:?*[\ and no leading "-" (git check-ref-format,
# made stricter). _check_ref_name adds the rules that a pattern cannot say simply.
_REF_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,254}")
# The names of the cache's folders: a repository's, then an entry's.
_REPO_DIR = re.compile(r"[0-9a-f]{16}")
_TEMP_PREFIX = ".tmp-"


class GitError(RuntimeError):
    """A git command failed or timed out. The message never holds the token."""


def check_repo_url(url: str, *, allow_file: bool = False) -> str:
    """Return ``url`` if git may fetch it, else raise ValueError.

    It must be an ``https://`` URL with a host and without credentials, or, with
    ``allow_file``, a ``file://`` URL. The error message never repeats the URL, which
    could hold a credential.
    """
    schemes = ("https", "file") if allow_file else ("https",)
    if not url.startswith(tuple(f"{scheme}://" for scheme in schemes)):
        allowed = " or ".join(f"{scheme}://" for scheme in schemes)
        raise ValueError(f"the repository URL must be an {allowed} URL")
    if len(url) > _MAX_URL_CHARS:
        raise ValueError(f"the repository URL is longer than {_MAX_URL_CHARS} characters")
    if any(char.isspace() or not char.isprintable() for char in url):
        raise ValueError("the repository URL must not contain whitespace or control characters")
    parts = urlsplit(url)
    if parts.scheme == "https":
        if "@" in parts.netloc:
            raise ValueError("the repository URL must not hold credentials")
        if not parts.hostname:
            raise ValueError("the repository URL must name a host")
    elif not parts.path:
        raise ValueError("the file:// URL must name a folder")
    return url


def check_commit(commit: str) -> str:
    """Return ``commit`` if git may fetch it, else raise ValueError.

    It must be a full 40-digit hex SHA (returned in lower case), or a branch or tag
    name made of letters, digits and ``._/-`` that starts with a letter or digit and
    that git accepts as a ref name. An abbreviated SHA (7 to 39 digits) passes as such
    a name, but git cannot fetch an abbreviated SHA: its fetch fails unless a branch
    or tag has that name.
    """
    if _FULL_SHA_ANY_CASE.fullmatch(commit):
        return commit.lower()
    if _REF_NAME.fullmatch(commit) and _check_ref_name(commit):
        return commit
    raise ValueError(
        "the commit must be a hex SHA or a branch or tag name (a safe ref name: letters, "
        "digits and ._/- only)"
    )


def _check_ref_name(name: str) -> bool:
    """The rules of git check-ref-format that ``_REF_NAME`` does not cover."""
    return (
        ".." not in name
        and "//" not in name
        and not name.endswith(("/", "."))
        and not any(part.startswith(".") or part.endswith(".lock") for part in name.split("/"))
    )


class GitSourceProvider(SourceProvider):
    """Checks out ``https://`` repositories with the git CLI into ``cache_dir``.

    ``token`` (``APP_GIT_TOKEN``) is sent as a Bearer header when set.
    ``max_cache_mb`` (``APP_SOURCE_CACHE_MAX_MB``) bounds the cache. ``timeout_s`` caps
    each git call. ``allow_file_urls`` also accepts ``file://`` URLs, for tests.
    Building the provider starts no process and touches no file.
    """

    def __init__(
        self,
        cache_dir: Path,
        token: SecretStr | None,
        max_cache_mb: int,
        *,
        timeout_s: float = DEFAULT_GIT_TIMEOUT_S,
        allow_file_urls: bool = False,
    ) -> None:
        self._cache_dir = cache_dir
        self._token = token
        self._max_cache_bytes = max_cache_mb * 1024 * 1024
        self._timeout_s = timeout_s
        self._allow_file_urls = allow_file_urls

    async def checkout(self, ref: SourceRef) -> Path:
        """The cache entry that holds ``ref.repo_url`` at ``ref.commit``.

        Raises ValueError for a URL or commit that ``check_repo_url`` or
        ``check_commit`` rejects, before any git process starts, and GitError when a
        git command fails or times out.
        """
        url = check_repo_url(ref.repo_url, allow_file=self._allow_file_urls)
        commit = check_commit(ref.commit)
        repo_dir = self._cache_dir / hashlib.sha256(url.encode()).hexdigest()[:16]
        if _FULL_SHA.fullmatch(commit):
            entry = repo_dir / commit
            if entry.is_dir():
                return self._reuse(entry, url)
        temp = _make_temp_dir(repo_dir)
        placed = False
        try:
            entry = await self._fetch(temp, repo_dir, url, commit)
            if not entry.is_dir():
                await self._git(temp, "checkout", "-q", "--detach", "FETCH_HEAD")
                placed = _place(temp, entry)
        finally:
            if not placed:
                await asyncio.to_thread(shutil.rmtree, temp, ignore_errors=True)
        if not placed:
            # The entry exists: an earlier checkout of the same SHA (a ref that is not a
            # full SHA is always fetched), or another run that renamed its checkout first.
            return self._reuse(entry, url)
        _log.info("source.checkout.fetched", repo_url=url, ref=commit, commit=entry.name)
        await asyncio.to_thread(_evict, self._cache_dir, entry, self._max_cache_bytes)
        return entry

    async def _fetch(self, temp: Path, repo_dir: Path, url: str, commit: str) -> Path:
        """Fetch ``commit`` into the empty folder ``temp``; return its entry in ``repo_dir``."""
        await self._git(temp, "init", "-q")
        await self._git(temp, "remote", "add", "--", "origin", url)
        await self._git(temp, "fetch", "-q", "--no-tags", "--depth", "1", "--", "origin", commit)
        resolved = await self._git(
            temp, "rev-parse", "--verify", "--end-of-options", "FETCH_HEAD^{commit}"
        )
        sha = resolved.strip()
        if not _FULL_SHA.fullmatch(sha) or (_FULL_SHA.fullmatch(commit) and sha != commit):
            raise GitError(f"git fetched {sha!r}, not the commit that was asked for")
        return repo_dir / sha

    def _reuse(self, entry: Path, url: str) -> Path:
        """Mark ``entry`` as just used, for the least-recently-used eviction."""
        os.utime(entry)
        _log.info("source.checkout.cached", repo_url=url, commit=entry.name)
        return entry

    async def _git(self, cwd: Path, *args: str) -> str:
        """Run ``git *args`` in ``cwd`` and return its output; raise GitError on failure."""
        process = await asyncio.create_subprocess_exec(
            "git",
            *args,
            cwd=cwd,
            env=self._env(),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            # Its own process group, so a timeout also stops git's helper processes.
            start_new_session=True,
        )
        try:
            async with asyncio.timeout(self._timeout_s):
                stdout, stderr = await process.communicate()
        except TimeoutError:
            raise GitError(f"git {args[0]} timed out after {self._timeout_s:g} s") from None
        finally:
            if process.returncode is None:
                _kill_group(process.pid)
                await process.wait()
        if process.returncode != 0:
            raise GitError(
                f"git {args[0]} failed with exit code {process.returncode}: "
                f"{self._scrub(stderr.decode(errors='replace'))}"
            )
        return stdout.decode(errors="replace")

    def _env(self) -> dict[str, str]:
        """The environment of a git process: the parent's, without its ``GIT_*`` variables."""
        env = {k: v for k, v in os.environ.items() if not k.upper().startswith("GIT_")}
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_ALLOW_PROTOCOL"] = "https:file" if self._allow_file_urls else "https"
        token = self._token_value()
        if token:
            env["GIT_CONFIG_COUNT"] = "1"
            env["GIT_CONFIG_KEY_0"] = "http.extraHeader"
            env["GIT_CONFIG_VALUE_0"] = f"Authorization: Bearer {token}"
        return env

    def _token_value(self) -> str:
        return "" if self._token is None else self._token.get_secret_value()

    def _scrub(self, text: str) -> str:
        """Git's error output for a message: redacted, without the token, and cut short."""
        token = self._token_value()
        if token:
            text = text.replace(token, REDACTED)
        return redact(text.strip())[-_MAX_ERROR_CHARS:]


def _make_temp_dir(repo_dir: Path) -> Path:
    """A new, empty temporary folder in ``repo_dir``, which is made if needed."""
    repo_dir.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=_TEMP_PREFIX, dir=repo_dir))


def _place(temp: Path, entry: Path) -> bool:
    """Rename the checkout ``temp`` to ``entry``; False if another run's is there already.

    The rename is atomic: a reader sees no entry or a whole one.
    """
    try:
        temp.rename(entry)
    except OSError:
        if not entry.is_dir():
            raise
        return False
    os.utime(entry)
    return True


def _kill_group(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _entries(cache_dir: Path) -> list[Path]:
    """The cache's entries: ``<16 hex>/<40 hex>`` folders, not symbolic links."""
    if not cache_dir.is_dir():
        return []
    return [
        entry
        for repo_dir in cache_dir.iterdir()
        if _REPO_DIR.fullmatch(repo_dir.name) and repo_dir.is_dir() and not repo_dir.is_symlink()
        for entry in repo_dir.iterdir()
        if _FULL_SHA.fullmatch(entry.name) and entry.is_dir() and not entry.is_symlink()
    ]


def _size(folder: Path) -> int:
    """The bytes of the files below ``folder``; symbolic links are not followed."""
    total = 0
    for parent, _dirs, files in os.walk(folder):
        for name in files:
            try:
                total += os.lstat(os.path.join(parent, name)).st_size
            except OSError:
                pass
    return total


def _evict(cache_dir: Path, keep: Path, max_bytes: int) -> None:
    """Delete the least recently used entries, never ``keep``, until within ``max_bytes``.

    A repository folder that is left empty is deleted too.
    """
    entries = _entries(cache_dir)
    sizes = {entry: _size(entry) for entry in entries}
    total = sum(sizes.values())
    if total <= max_bytes:
        return
    for entry in sorted(entries, key=_mtime):
        if total <= max_bytes:
            break
        if entry == keep:
            continue
        shutil.rmtree(entry, ignore_errors=True)
        total -= sizes[entry]
        _log.info("source.cache.evicted", path=str(entry), bytes=sizes[entry])
        try:
            entry.parent.rmdir()
        except OSError:
            pass  # not empty


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return time.time()
