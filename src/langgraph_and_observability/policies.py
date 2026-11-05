"""Retry and timeout policies for graph nodes (spec §6.5 and §8.5).

LangGraph's ``default_retry_on`` does not suit this agent. It never retries
``TimeoutError`` (an ``OSError``), and it retries provider-SDK errors that it does not
recognise, 400s included. External calls get one retry layer: the client library's
own (botocore's standard mode, the Datadog and Kubernetes clients' retries). The
graph's ``RetryPolicy`` retries only what ``is_transient`` accepts: transient
infrastructure errors, and never a 4xx except a Kubernetes 429.

From the vendor SDKs, this module imports exception types only: the ones that
``is_transient`` accepts, and one that it must exclude. botocore's errors and the
Datadog client's ``ApiException`` are not transient here, because those clients have
already retried them. The Datadog async client retries only on HTTP status, so the
timeouts and dropped connections of its transport, aiosonic, are transient here: this
is their one retry layer.
"""

import socket
import ssl

import aiohttp
import aiosonic.exceptions as aiosonic_exc  # type: ignore[import-untyped]
import psycopg
from kubernetes_asyncio.client.exceptions import ApiException as KubernetesApiException
from langgraph.errors import NodeTimeoutError
from langgraph.types import RetryPolicy, TimeoutPolicy
from psycopg import errors as pg_errors

# Transient by type; subclasses count too. OSError is not on the list: botocore's
# connect, read, SSL and proxy errors subclass OSError (through botocore's vendored
# requests), and botocore has already retried them. None of them subclasses
# ConnectionError, TimeoutError, socket.gaierror or ssl.SSLEOFError.
_TRANSIENT_TYPES: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    # A DNS lookup that failed, and a TLS connection closed during the handshake. Both
    # subclass OSError only, and a retry often succeeds.
    socket.gaierror,
    ssl.SSLEOFError,
    # Raised by a node TimeoutPolicy. It does not subclass TimeoutError.
    NodeTimeoutError,
    # SerializationFailure and DeadlockDetected subclass OperationalError. They are
    # named because §6.5 names them.
    pg_errors.SerializationFailure,
    pg_errors.DeadlockDetected,
    psycopg.OperationalError,
    aiohttp.ClientConnectionError,
    # aiosonic, the Datadog async client's transport, which does not retry them. Its
    # timeouts subclass AiosonicError, not TimeoutError, and so do its connection errors:
    # a connection dropped before the response (ConnectionDisconnected), and a read or
    # write on a connection that is already closed (MissingReader/WriterException). Its
    # protocol and request errors (parsing, redirects, HTTP/2, decompression, status)
    # stay not transient.
    aiosonic_exc.ConnectTimeout,
    aiosonic_exc.ReadTimeout,
    aiosonic_exc.RequestTimeout,
    aiosonic_exc.ConnectionPoolAcquireTimeout,
    aiosonic_exc.ConnectionDisconnected,
    aiosonic_exc.MissingReaderException,
    aiosonic_exc.MissingWriterException,
)

# Subclasses of a transient type that are not transient. WebSocketMessageTooBig
# subclasses ConnectionDisconnected, but it is a protocol error: a frame over the size
# limit, which a retry would receive again.
_NOT_TRANSIENT_TYPES: tuple[type[BaseException], ...] = (aiosonic_exc.WebSocketMessageTooBig,)


def is_transient(exc: BaseException) -> bool:
    """Return True if a node that raised ``exc`` may run again.

    True for connection errors, timeouts (``TimeoutError`` and LangGraph's
    ``NodeTimeoutError``), failed DNS lookups (``socket.gaierror``), TLS connections
    closed early (``ssl.SSLEOFError``), Postgres serialization failures, deadlocks and operational
    errors, aiohttp connection errors, aiosonic timeouts and connection errors, and a
    Kubernetes ``ApiException`` with status 429 or 5xx. False for everything else, which
    includes all botocore errors, the Datadog client's ``ApiException`` and LangGraph's
    ``GraphDrained``.
    """
    if isinstance(exc, _NOT_TRANSIENT_TYPES):
        return False
    if isinstance(exc, _TRANSIENT_TYPES):
        return True
    if isinstance(exc, KubernetesApiException):
        # The stub types status as int, but it is None when the exception has no status.
        status = exc.status
        return isinstance(status, int) and (status == 429 or status >= 500)
    return False


def default_retry_policy() -> RetryPolicy:
    """The graph-wide retry policy: at most 3 attempts, only for transient errors."""
    return RetryPolicy(
        max_attempts=3,
        initial_interval=0.5,
        backoff_factor=2.0,
        jitter=True,
        retry_on=is_transient,
    )


def step_timeout(run_s: float) -> TimeoutPolicy:
    """A node timeout that caps one attempt of the node at ``run_s`` seconds.

    It sets ``TimeoutPolicy.run_timeout``, the hard wall-clock cap for a single node
    attempt. Progress signals and ``runtime.heartbeat()`` never refresh it, so a node
    that keeps streaming still stops at ``run_s``. ``idle_timeout`` stays unset: a step
    is bounded by its total time, not by its gaps between progress signals.

    The cap applies to each attempt. When it fires, LangGraph raises
    ``NodeTimeoutError``, which ``is_transient`` accepts, so the retry policy can run
    the node again.
    """
    return TimeoutPolicy(run_timeout=run_s)
