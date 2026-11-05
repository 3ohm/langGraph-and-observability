"""Retry and timeout policies for graph nodes (spec §6.5 and §8.5).

LangGraph's ``default_retry_on`` does not suit this template. It never retries
``TimeoutError`` (an ``OSError``), and it retries provider-SDK errors that it does not
recognise, 400s included. External calls get one retry layer: the client library's
own, when it has one. The graph's ``RetryPolicy`` retries only what its ``retry_on``
predicate accepts: transient infrastructure errors, never a client's error that the
client has already retried.

``is_transient`` here accepts the transient errors of the standard library, LangGraph
and psycopg, and imports no vendor SDK, so it stays when a fork removes an agent and
its clients. An agent whose clients raise other transient errors, such as their
transports' timeouts, writes its own predicate that also accepts what this one
accepts, and gives it to ``default_retry_policy(retry_on=...)``.
"""

import socket
import ssl
from collections.abc import Callable

import psycopg
from langgraph.errors import NodeTimeoutError
from langgraph.types import RetryPolicy, TimeoutPolicy
from psycopg import errors as pg_errors

# Transient by type; subclasses count too. OSError is not on the list: it also holds
# errors that a retry cannot fix (a missing file, a denied permission), and client
# libraries subclass it for errors that they have already retried.
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
)


def is_transient(exc: BaseException) -> bool:
    """Return True if a node that raised ``exc`` may run again.

    True for connection errors, timeouts (``TimeoutError`` and LangGraph's
    ``NodeTimeoutError``), failed DNS lookups (``socket.gaierror``), TLS connections
    closed early (``ssl.SSLEOFError``), and Postgres serialization failures, deadlocks
    and operational errors. False for everything else, which includes every other
    ``OSError`` and LangGraph's ``GraphDrained``.
    """
    return isinstance(exc, _TRANSIENT_TYPES)


def default_retry_policy(
    retry_on: Callable[[BaseException], bool] = is_transient,
) -> RetryPolicy:
    """The graph-wide retry policy: at most 3 attempts, only for transient errors.

    ``retry_on`` says which errors are transient. An agent with its own predicate gives
    it here; the attempts and the backoff stay the same.
    """
    return RetryPolicy(
        max_attempts=3,
        initial_interval=0.5,
        backoff_factor=2.0,
        jitter=True,
        retry_on=retry_on,
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
