import aiohttp
import botocore.exceptions as botocore_exc  # type: ignore[import-untyped]
import datadog_api_client.exceptions as datadog_exc
import psycopg
import pydantic
import pytest
from kubernetes_asyncio.client.exceptions import ApiException as KubernetesApiException
from langgraph.errors import GraphBubbleUp, GraphDrained, NodeTimeoutError
from langgraph.types import RetryPolicy, TimeoutPolicy
from psycopg import errors as pg_errors

from langgraph_postgres_starter.policies import default_retry_policy, is_transient, step_timeout


def _validation_error() -> pydantic.ValidationError:
    try:
        pydantic.TypeAdapter(int).validate_python("not a number")
    except pydantic.ValidationError as exc:
        return exc
    raise AssertionError("expected a ValidationError")


def _datadog_error(status: int) -> Exception:
    # The Datadog client ships py.typed, but this constructor has no annotations.
    return datadog_exc.ApiException(status=status)  # type: ignore[no-untyped-call]


BEDROCK = "https://bedrock-runtime.us-east-1.amazonaws.com"

# The table from the plan (Task 6), with a few subclasses and neighbours added.
TABLE = [
    # Transient: connection errors and timeouts.
    pytest.param(ConnectionError("reset"), True, id="ConnectionError"),
    pytest.param(ConnectionResetError(), True, id="ConnectionResetError"),
    pytest.param(TimeoutError(), True, id="TimeoutError"),
    pytest.param(
        NodeTimeoutError("collect", 30.5, kind="run", run_timeout=30.0),
        True,
        id="langgraph-NodeTimeoutError",
    ),
    # Transient: Postgres.
    pytest.param(pg_errors.SerializationFailure(), True, id="psycopg-SerializationFailure"),
    pytest.param(pg_errors.DeadlockDetected(), True, id="psycopg-DeadlockDetected"),
    pytest.param(psycopg.OperationalError("server closed"), True, id="psycopg-OperationalError"),
    # Transient: aiohttp (the Kubernetes client's transport).
    pytest.param(aiohttp.ClientConnectionError(), True, id="aiohttp-ClientConnectionError"),
    pytest.param(aiohttp.ServerDisconnectedError(), True, id="aiohttp-ServerDisconnectedError"),
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
        botocore_exc.SSLError(endpoint_url=BEDROCK, error="handshake"),
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
    # Programming and data errors.
    pytest.param(ValueError("bad"), False, id="ValueError"),
    pytest.param(_validation_error(), False, id="pydantic-ValidationError"),
    pytest.param(FileNotFoundError("missing"), False, id="FileNotFoundError"),
    # LangGraph control flow.
    pytest.param(GraphDrained(), False, id="langgraph-GraphDrained"),
    pytest.param(GraphBubbleUp(), False, id="langgraph-GraphBubbleUp"),
]


@pytest.mark.parametrize(("exc", "expected"), TABLE)
def test_is_transient_table(exc, expected):
    assert is_transient(exc) is expected


def test_default_policy():
    p = default_retry_policy()
    assert isinstance(p, RetryPolicy)
    assert p.max_attempts == 3 and p.retry_on is is_transient
    assert p.initial_interval == 0.5 and p.backoff_factor == 2.0 and p.jitter is True


def test_step_timeout_caps_one_node_attempt():
    p = step_timeout(30)
    assert isinstance(p, TimeoutPolicy)
    assert p.run_timeout == 30 and p.idle_timeout is None
