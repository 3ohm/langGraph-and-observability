"""The example's retry predicate: the core's errors and its vendor clients' (P12)."""

import inspect
import socket
import ssl

import aiohttp
import aiosonic.exceptions as aiosonic_exc  # type: ignore[import-untyped]
import botocore.exceptions as botocore_exc
import datadog_api_client.exceptions as datadog_exc
import psycopg
import pytest
from kubernetes_asyncio.client.exceptions import ApiException as KubernetesApiException
from langgraph.errors import GraphDrained, NodeTimeoutError

from langgraph_and_observability import policies as core_policies
from langgraph_and_observability.agents.incident_triage.policies import is_transient


def _datadog_error(status: int) -> Exception:
    # The Datadog client ships py.typed, but this constructor has no annotations.
    return datadog_exc.ApiException(status=status)  # type: ignore[no-untyped-call]


BEDROCK = "https://bedrock-runtime.us-east-1.amazonaws.com"

# What the core accepts, and what it rejects, the example does too.
CORE = [
    pytest.param(ConnectionError("reset"), True, id="ConnectionError"),
    pytest.param(TimeoutError(), True, id="TimeoutError"),
    pytest.param(
        socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution"),
        True,
        id="socket-gaierror",
    ),
    pytest.param(
        ssl.SSLEOFError(8, "EOF occurred in violation of protocol"), True, id="SSLEOFError"
    ),
    pytest.param(
        NodeTimeoutError("collect", 30.5, kind="run", run_timeout=30.0),
        True,
        id="langgraph-NodeTimeoutError",
    ),
    pytest.param(psycopg.OperationalError("server closed"), True, id="psycopg-OperationalError"),
    pytest.param(ValueError("bad"), False, id="ValueError"),
    pytest.param(GraphDrained(), False, id="langgraph-GraphDrained"),
]

# The vendor clients' errors, which the core does not know.
TABLE = [
    # Transient: aiohttp (the Kubernetes client's transport).
    pytest.param(aiohttp.ClientConnectionError(), True, id="aiohttp-ClientConnectionError"),
    pytest.param(aiohttp.ServerDisconnectedError(), True, id="aiohttp-ServerDisconnectedError"),
    # Transient: aiosonic (the Datadog async client's transport, which does not retry).
    pytest.param(aiosonic_exc.ConnectTimeout(), True, id="aiosonic-ConnectTimeout"),
    pytest.param(aiosonic_exc.ReadTimeout(), True, id="aiosonic-ReadTimeout"),
    pytest.param(aiosonic_exc.RequestTimeout(), True, id="aiosonic-RequestTimeout"),
    pytest.param(
        aiosonic_exc.ConnectionPoolAcquireTimeout(),
        True,
        id="aiosonic-ConnectionPoolAcquireTimeout",
    ),
    pytest.param(aiosonic_exc.ConnectionDisconnected(), True, id="aiosonic-ConnectionDisconnected"),
    pytest.param(
        aiosonic_exc.MissingReaderException("reader not set."),
        True,
        id="aiosonic-MissingReaderException",
    ),
    pytest.param(
        aiosonic_exc.MissingWriterException("writer not set."),
        True,
        id="aiosonic-MissingWriterException",
    ),
    # aiosonic protocol and request errors are not transient, nor is its base class.
    pytest.param(
        aiosonic_exc.HttpParsingError("missing hostname"), False, id="aiosonic-HttpParsingError"
    ),
    pytest.param(
        aiosonic_exc.WebSocketMessageTooBig("frame too big"),
        False,
        id="aiosonic-WebSocketMessageTooBig",
    ),
    pytest.param(aiosonic_exc.MaxRedirects(), False, id="aiosonic-MaxRedirects"),
    pytest.param(aiosonic_exc.AiosonicError(), False, id="aiosonic-AiosonicError"),
    # Kubernetes: 429 and 5xx are transient; other 4xx and no status are not.
    pytest.param(KubernetesApiException(status=429), True, id="k8s-429"),
    pytest.param(KubernetesApiException(status=500), True, id="k8s-500"),
    pytest.param(KubernetesApiException(status=503), True, id="k8s-503"),
    pytest.param(KubernetesApiException(status=400), False, id="k8s-400"),
    pytest.param(KubernetesApiException(status=403), False, id="k8s-403"),
    pytest.param(KubernetesApiException(status=404), False, id="k8s-404"),
    pytest.param(KubernetesApiException(status=409), False, id="k8s-409"),
    pytest.param(KubernetesApiException(), False, id="k8s-no-status"),
    # botocore has already retried these (standard mode).
    pytest.param(
        botocore_exc.ClientError({"Error": {"Code": "ThrottlingException"}}, "InvokeModel"),
        False,
        id="botocore-ClientError-throttling",
    ),
    pytest.param(
        botocore_exc.ClientError({"Error": {"Code": "ServiceUnavailableException"}}, "InvokeModel"),
        False,
        id="botocore-ClientError-unavailable",
    ),
    pytest.param(
        botocore_exc.EndpointConnectionError(endpoint_url=BEDROCK),
        False,
        id="botocore-EndpointConnectionError",
    ),
    # These four subclass OSError (through botocore's vendored requests).
    pytest.param(
        botocore_exc.ConnectTimeoutError(endpoint_url=BEDROCK),
        False,
        id="botocore-ConnectTimeoutError",
    ),
    pytest.param(
        botocore_exc.ReadTimeoutError(endpoint_url=BEDROCK), False, id="botocore-ReadTimeoutError"
    ),
    pytest.param(
        botocore_exc.SSLError(endpoint_url=BEDROCK, error=Exception("handshake")),
        False,
        id="botocore-SSLError",
    ),
    pytest.param(
        botocore_exc.ProxyConnectionError(proxy_url="http://proxy:3128", error="refused"),
        False,
        id="botocore-ProxyConnectionError",
    ),
    pytest.param(
        botocore_exc.ConnectionClosedError(endpoint_url=BEDROCK),
        False,
        id="botocore-ConnectionClosedError",
    ),
    # The Datadog client retries 429 and 5xx itself.
    pytest.param(_datadog_error(429), False, id="datadog-429"),
    pytest.param(_datadog_error(503), False, id="datadog-503"),
]


@pytest.mark.parametrize(("exc", "expected"), CORE)
def test_accepts_what_the_core_accepts(exc, expected):
    assert core_policies.is_transient(exc) is expected
    assert is_transient(exc) is expected


@pytest.mark.parametrize(("exc", "expected"), TABLE)
def test_is_transient_table(exc, expected):
    assert is_transient(exc) is expected


@pytest.mark.parametrize(("exc", "expected"), TABLE)
def test_core_rejects_the_vendor_errors(exc, expected):
    # The vendor errors are the example's: the core policy accepts none of them.
    assert core_policies.is_transient(exc) is False


@pytest.mark.parametrize("module", [botocore_exc, datadog_exc], ids=["botocore", "datadog"])
def test_no_client_error_subclasses_the_transient_os_errors(module):
    # botocore and the Datadog client retry their own errors (one retry layer), so none
    # of them may count as transient by subclassing gaierror or SSLEOFError.
    errors = [obj for _, obj in inspect.getmembers(module, inspect.isclass)]
    assert errors
    assert [e for e in errors if issubclass(e, (socket.gaierror, ssl.SSLEOFError))] == []
