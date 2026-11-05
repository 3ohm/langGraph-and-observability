"""Which errors the example's graph retries (P12; spec §6.5).

``is_transient`` accepts what the core's ``policies.is_transient`` accepts, and the
transient errors of the example's vendor clients. ``graph.py`` gives it to
``default_retry_policy(retry_on=...)``. It accepts no 4xx, except a Kubernetes 429.

From the vendor SDKs, this module imports exception types only: the ones that
``is_transient`` accepts, and one that it must exclude. botocore's errors and the
Datadog client's ``ApiException`` are not transient here, because those clients have
already retried them. botocore's connect, read, SSL and proxy errors subclass
``OSError`` (through botocore's vendored requests), which the core does not accept;
none of them subclasses ``ConnectionError``, ``TimeoutError``, ``socket.gaierror`` or
``ssl.SSLEOFError``. The Datadog async client retries only on HTTP status, so the
timeouts and dropped connections of its transport, aiosonic, are transient here: this
is their one retry layer.
"""

import aiohttp
import aiosonic.exceptions as aiosonic_exc  # type: ignore[import-untyped]
from kubernetes_asyncio.client.exceptions import ApiException as KubernetesApiException

from langgraph_and_observability import policies

# Transient by type; subclasses count too.
_TRANSIENT_TYPES: tuple[type[BaseException], ...] = (
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
    """Return True if a node of the example's graph that raised ``exc`` may run again.

    True for what the core's ``policies.is_transient`` accepts, aiohttp connection
    errors, aiosonic timeouts and connection errors, and a Kubernetes ``ApiException``
    with status 429 or 5xx. False for everything else, which includes all botocore
    errors and the Datadog client's ``ApiException``.
    """
    if isinstance(exc, _NOT_TRANSIENT_TYPES):
        return False
    if policies.is_transient(exc) or isinstance(exc, _TRANSIENT_TYPES):
        return True
    if isinstance(exc, KubernetesApiException):
        # The stub types status as int, but it is None when the exception has no status.
        status = exc.status
        return isinstance(status, int) and (status == 429 or status >= 500)
    return False
