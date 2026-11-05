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
with the oldest modification time is deleted. A checkout that is reused gets a new
modification time, so the order is least recently used. An entry used less than
``min_age_s`` ago (``build_adapters`` passes the run deadline) is never deleted, nor
is the one just checked out: a run may still be reading it. When only such entries are
left, the cache stays over budget for that pass, and ``source.cache_over_budget`` logs
the sizes. An entry that another run deletes between the check and the touch is a
cache miss: the commit is fetched again. Only folders with the names above count:
other files in ``cache_dir`` are left alone.

Untrusted input (ruling R59): a ref's URL and commit come from the cluster
(annotations, image labels, the source map), and anyone who can edit a workload's
annotations sets them. So ``check_repo_url`` and ``check_commit`` validate them before
any git process starts, and ``--`` comes before git's positional arguments. A URL must
be ``https://`` on a host in ``allowed_hosts`` (``APP_GIT_ALLOWED_HOSTS``, default
``["github.com"]``), without credentials, a port other than 443, a query or a fragment.
Without the allowlist, an annotation could send the token to any host or make the
worker fetch from an internal one. ``file://`` URLs pass only with
``allow_file_urls``, which is for tests.

The token: git gets it in the environment as ``http.https://<host>/.extraHeader:
Authorization: Bearer <token>`` (``GIT_CONFIG_COUNT``, ``GIT_CONFIG_KEY_n`` and
``GIT_CONFIG_VALUE_n``), so it is never in an argument list that ``ps`` shows, never in
the URL, never in ``.git/config`` and never logged, and git sends it only to the
repository's own host. GitHub accepts a token as a Bearer header for git over HTTPS;
GitLab's git HTTP endpoint ignores Bearer and needs Basic auth (checked 2026-11-04), so
a GitLab token does not work this way.

Each git process gets a minimal environment: of the parent's variables only ``PATH``,
``HOME``, ``LANG``, ``LC_ALL``, ``SSL_CERT_FILE``, ``SSL_CERT_DIR`` and the proxy
variables (``HTTP_PROXY``, ``HTTPS_PROXY``, ``NO_PROXY``, in upper and lower case), so
the worker's own secrets (``APP_GIT_TOKEN``, ``AWS_*``, ``DD_*``) never reach git or its
helpers. Git reads no system or global config file (``GIT_CONFIG_NOSYSTEM=1``,
``GIT_CONFIG_GLOBAL`` the null device), so a ``url.<base>.insteadOf`` or a credential
helper there cannot change where it fetches or what it sends. It also gets
``http.followRedirects=false`` (the same ``GIT_CONFIG_*`` variables, with or without a
token), ``GIT_TERMINAL_PROMPT=0`` (fail rather than prompt) and ``GIT_ALLOW_PROTOCOL``
set to the URL's own scheme. Each git call is stopped after ``timeout_s``: its process
group is killed and the call raises ``GitError``.

A worker that is killed during a checkout leaves its temporary folder; the eviction
after a later checkout deletes such a folder once it is older than ``min_age_s``.
"""

import asyncio
import hashlib
import os
import re
import shutil
import signal
import tempfile
import time
from collections.abc import Collection
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import SecretStr

from langgraph_and_observability.agents.incident_triage.adapters.source.interface import (
    SourceProvider,
    SourceRef,
)
from langgraph_and_observability.logging import get_logger
from langgraph_and_observability.redaction import REDACTED, redact

_log = get_logger(__name__)

# The longest that one git call may run, in seconds.
DEFAULT_GIT_TIMEOUT_S = 120.0
# An entry used less than this long ago is never evicted: the default run deadline.
DEFAULT_MIN_AGE_S = 900.0
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
# The parent's variables that git gets: to run, and to reach a host through a proxy with
# the system's CA certificates. Nothing else is passed on.
_PASSED_ENV = (
    "PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
)


class GitError(RuntimeError):
    """A git command failed or timed out. The message never holds the token."""


def normalize_host(host: str) -> str:
    """A host name as the allowlist compares it: lower case, without a trailing dot."""
    return host.lower().removesuffix(".")


def check_repo_url(url: str, *, allowed_hosts: Collection[str], allow_file: bool = False) -> str:
    """Return the canonical form of ``url`` if git may fetch it, else raise ValueError.

    It must be an ``https://`` URL whose host is in ``allowed_hosts``
    (``APP_GIT_ALLOWED_HOSTS``; both sides compared by ``normalize_host``), with a
    repository path, and with no credentials, no port other than 443, no query and no
    fragment. The canonical form is ``https://<normalized host><path>``. With
    ``allow_file`` (tests only), a ``file://`` URL with no host passes unchanged.

    Error messages may name the host, but never repeat the URL, which could hold a
    credential.
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
    if parts.scheme == "file":
        if parts.netloc or not parts.path:
            raise ValueError("a file:// URL must name a local folder and no host")
        return url
    if "@" in parts.netloc:
        raise ValueError("the repository URL must not hold credentials")
    if not parts.hostname:
        raise ValueError("the repository URL must name a host")
    host = normalize_host(parts.hostname)
    try:
        port = parts.port
    except ValueError:
        raise ValueError(f"the repository URL for {host!r} has an invalid port") from None
    if port not in (None, 443):
        raise ValueError(f"the repository URL for {host!r} must not name a port other than 443")
    if host not in {normalize_host(allowed) for allowed in allowed_hosts}:
        raise ValueError(
            f"the repository host {host!r} is not in the allowed hosts (APP_GIT_ALLOWED_HOSTS)"
        )
    if parts.query or parts.fragment or url.endswith(("?", "#")):
        raise ValueError(f"the repository URL for {host!r} must not have a query or fragment")
    if parts.path.strip("/") == "":
        raise ValueError(f"the repository URL for {host!r} must name a repository path")
    return f"https://{host}{parts.path}"


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

    ``token`` (``APP_GIT_TOKEN``) is sent as a Bearer header to the repository's host,
    when set. ``allowed_hosts`` (``APP_GIT_ALLOWED_HOSTS``) are the only hosts it
    fetches from. ``max_cache_mb`` (``APP_SOURCE_CACHE_MAX_MB``) bounds the cache.
    ``min_age_s`` is how long after its last use an entry is safe from eviction; it must
    cover the longest run (``APP_RUN_DEADLINE_SECONDS``). ``timeout_s`` caps each git
    call. ``allow_file_urls`` also accepts ``file://`` URLs; it is for tests, and
    ``build_adapters`` never sets it. Building the provider starts no process and
    touches no file.
    """

    def __init__(
        self,
        cache_dir: Path,
        token: SecretStr | None,
        max_cache_mb: int,
        *,
        allowed_hosts: Collection[str],
        min_age_s: float = DEFAULT_MIN_AGE_S,
        timeout_s: float = DEFAULT_GIT_TIMEOUT_S,
        allow_file_urls: bool = False,
    ) -> None:
        self._cache_dir = cache_dir
        self._token = token
        self._allowed_hosts = tuple(allowed_hosts)
        self._max_cache_bytes = max_cache_mb * 1024 * 1024
        self._min_age_s = min_age_s
        self._timeout_s = timeout_s
        self._allow_file_urls = allow_file_urls

    async def checkout(self, ref: SourceRef) -> Path:
        """The cache entry that holds ``ref.repo_url`` at ``ref.commit``.

        Raises ValueError for a URL or commit that ``check_repo_url`` or
        ``check_commit`` rejects, before any git process starts, and GitError when a
        git command fails or times out.
        """
        url = check_repo_url(
            ref.repo_url, allowed_hosts=self._allowed_hosts, allow_file=self._allow_file_urls
        )
        commit = check_commit(ref.commit)
        repo_dir = self._cache_dir / hashlib.sha256(url.encode()).hexdigest()[:16]
        if _FULL_SHA.fullmatch(commit):
            cached = self._reuse(repo_dir / commit, url)
            if cached is not None:
                return cached
        env = self._env(urlsplit(url).hostname)
        temp = _make_temp_dir(repo_dir)
        placed = False
        try:
            entry = await self._fetch(temp, repo_dir, url, commit, env)
            # An earlier checkout of the same SHA (a ref that is not a full SHA is always
            # fetched), or one that another run made while this one fetched.
            cached = self._reuse(entry, url)
            if cached is not None:
                return cached
            await self._git(env, temp, "checkout", "-q", "--detach", "FETCH_HEAD")
            placed = _place(temp, entry)
        finally:
            if not placed:
                await asyncio.to_thread(shutil.rmtree, temp, ignore_errors=True)
        if not placed:
            # Another run renamed its checkout into place first. It was just made, so no
            # eviction deletes it within min_age_s.
            cached = self._reuse(entry, url)
            if cached is None:
                raise GitError(f"the checkout of {entry.name} was deleted as it was made")
            return cached
        _log.info("source.checkout.fetched", repo_url=url, ref=commit, commit=entry.name)
        await asyncio.to_thread(
            _evict, self._cache_dir, entry, self._max_cache_bytes, self._min_age_s
        )
        return entry

    async def _fetch(
        self, temp: Path, repo_dir: Path, url: str, commit: str, env: dict[str, str]
    ) -> Path:
        """Fetch ``commit`` into the empty folder ``temp``; return its entry in ``repo_dir``."""
        await self._git(env, temp, "init", "-q")
        await self._git(env, temp, "remote", "add", "--", "origin", url)
        fetch = ("fetch", "-q", "--no-tags", "--depth", "1", "--", "origin", commit)
        await self._git(env, temp, *fetch)
        resolved = await self._git(
            env, temp, "rev-parse", "--verify", "--end-of-options", "FETCH_HEAD^{commit}"
        )
        sha = resolved.strip()
        if not _FULL_SHA.fullmatch(sha) or (_FULL_SHA.fullmatch(commit) and sha != commit):
            raise GitError(f"git fetched {sha!r}, not the commit that was asked for")
        return repo_dir / sha

    def _reuse(self, entry: Path, url: str) -> Path | None:
        """Mark ``entry`` as just used and return it; None if it does not exist (a miss).

        The touch both orders the least-recently-used eviction and keeps ``entry`` safe
        from it for ``min_age_s``.
        """
        if not entry.is_dir():
            return None
        try:
            os.utime(entry)
        except FileNotFoundError:
            # Another run's eviction deleted it between the check and the touch.
            return None
        _log.info("source.checkout.cached", repo_url=url, commit=entry.name)
        return entry

    async def _git(self, env: dict[str, str], cwd: Path, *args: str) -> str:
        """Run ``git *args`` in ``cwd`` and return its output; raise GitError on failure."""
        process = await asyncio.create_subprocess_exec(
            "git",
            *args,
            cwd=cwd,
            env=env,
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

    def _env(self, host: str | None) -> dict[str, str]:
        """The environment of the git processes for a repository on ``host``.

        It holds the parent's ``_PASSED_ENV`` variables that are set, plus the settings
        in the module docstring. ``host`` is None for a ``file://`` URL, which gets no
        header.
        """
        env = {name: os.environ[name] for name in _PASSED_ENV if name in os.environ}
        env["GIT_CONFIG_NOSYSTEM"] = "1"
        env["GIT_CONFIG_GLOBAL"] = os.devnull
        env["GIT_TERMINAL_PROMPT"] = "0"
        # Only the URL's own transport, so an https fetch never falls back to another.
        env["GIT_ALLOW_PROTOCOL"] = "file" if host is None else "https"
        config: list[tuple[str, str]] = []
        token = self._token_value()
        if token and host is not None:
            config.append((f"http.https://{host}/.extraHeader", f"Authorization: Bearer {token}"))
        config.append(("http.followRedirects", "false"))
        env["GIT_CONFIG_COUNT"] = str(len(config))
        for index, (key, value) in enumerate(config):
            env[f"GIT_CONFIG_KEY_{index}"] = key
            env[f"GIT_CONFIG_VALUE_{index}"] = value
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


def _temp_dirs(cache_dir: Path) -> list[Path]:
    """The checkouts' temporary folders: ``<16 hex>/.tmp-*`` folders, not symbolic links."""
    if not cache_dir.is_dir():
        return []
    return [
        temp
        for repo_dir in cache_dir.iterdir()
        if _REPO_DIR.fullmatch(repo_dir.name) and repo_dir.is_dir() and not repo_dir.is_symlink()
        for temp in repo_dir.iterdir()
        if temp.name.startswith(_TEMP_PREFIX) and temp.is_dir() and not temp.is_symlink()
    ]


def _evict(cache_dir: Path, keep: Path, max_bytes: int, min_age_s: float) -> None:
    """Delete stale temporary folders, then old entries until the cache fits ``max_bytes``.

    A temporary folder older than ``min_age_s`` was left by a checkout that never ended
    (a worker killed during it), so it is deleted, whatever the budget. Entries go least
    recently used first; it never deletes ``keep`` or an entry used less than
    ``min_age_s`` ago. If only those are left over budget, it logs
    ``source.cache_over_budget`` once, with sizes only. A repository folder that is left
    empty is deleted too.
    """
    now = time.time()
    for temp in _temp_dirs(cache_dir):
        if now - _mtime(temp) >= min_age_s:
            shutil.rmtree(temp, ignore_errors=True)
            _log.info("source.cache.temp_deleted", path=str(temp))
    entries = _entries(cache_dir)
    sizes = {entry: _size(entry) for entry in entries}
    total = sum(sizes.values())
    if total <= max_bytes:
        return
    for entry in sorted(entries, key=_mtime):
        if total <= max_bytes:
            break
        if entry == keep or now - _mtime(entry) < min_age_s:
            continue
        shutil.rmtree(entry, ignore_errors=True)
        total -= sizes[entry]
        _log.info("source.cache.evicted", path=str(entry), bytes=sizes[entry])
        try:
            entry.parent.rmdir()
        except OSError:
            pass  # not empty
    if total > max_bytes:
        _log.warning(
            "source.cache_over_budget", bytes=total, max_bytes=max_bytes, min_age_s=min_age_s
        )


def _mtime(path: Path) -> float:
    """``path``'s modification time; now if it is gone, so it counts as just used."""
    try:
        return path.stat().st_mtime
    except OSError:
        return time.time()
