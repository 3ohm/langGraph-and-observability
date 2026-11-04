"""The real Datadog adapter, against a mocked EventsApi or a mocked transport (spec §9.4).

Most tests swap ``EventsApi`` in the adapter's module for ``_EventsApi``, which records
the API client and the request body and answers with the client's own models. The
transport tests go one level down: they swap ``aiosonic.HTTPClient.request``, the HTTP
transport of the client's ``AsyncApiClient``, so the client itself serializes the
request, retries and parses the answer. Nothing touches the network.
"""

import asyncio
import json
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Self

import aiosonic  # type: ignore[import-untyped]
import pytest
from datadog_api_client import AsyncApiClient
from datadog_api_client.exceptions import ApiException, ForbiddenException, ServiceException
from datadog_api_client.v2.model.event_attributes import EventAttributes
from datadog_api_client.v2.model.event_response import EventResponse
from datadog_api_client.v2.model.event_response_attributes import EventResponseAttributes
from datadog_api_client.v2.model.event_type import EventType
from datadog_api_client.v2.model.events_list_request import EventsListRequest
from datadog_api_client.v2.model.events_list_response import EventsListResponse
from datadog_api_client.v2.model.events_query_filter import EventsQueryFilter
from datadog_api_client.v2.model.events_request_page import EventsRequestPage
from datadog_api_client.v2.model.events_sort import EventsSort

from langgraph_postgres_starter.agents.incident_triage.adapters import (
    _build_datadog,
    build_adapters,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.call_log import NO_CALL_LOG
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog import real
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.fake import FakeDatadog
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.interface import DdEvent
from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.real import RealDatadog
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_postgres_starter.policies import is_transient
from langgraph_postgres_starter.settings import load_settings

API_KEY = "dd-api-key-for-tests"
APP_KEY = "dd-app-key-for-tests"
POD = "orders-7d9f8b6c5d-x2kqp"
QUERY = f"kube_namespace:shop (pod_name:{POD} OR kube_deployment:orders)"
START = datetime(2026, 11, 2, 9, 30, tzinfo=UTC)
END = datetime(2026, 11, 2, 10, 5, tzinfo=UTC)
# START and END in milliseconds since the epoch.
START_MS = "1793611800000"
END_MS = "1793613900000"
SEEN = datetime(2026, 11, 2, 10, 4, 31, 120000, tzinfo=UTC)


def _settings(**overrides: Any):
    values: dict[str, Any] = {
        "adapters": "real",
        "dd_api_key": API_KEY,
        "dd_app_key": APP_KEY,
        "dd_site": "datadoghq.eu",
    }
    return load_settings(**(values | overrides))


def _response(*events: EventResponse) -> EventsListResponse:
    return EventsListResponse(data=list(events))


def _event_response(**attributes: Any) -> EventResponse:
    """An event as the client parses it: the ``EventsApi`` view of SEEN's BackOff event."""
    inner = attributes.pop("inner", {"title": f"Events from the {POD} Pod"})
    values: dict[str, Any] = {
        "message": f"BackOff: Back-off restarting failed container orders in pod {POD}",
        "tags": ["kube_namespace:shop", f"pod_name:{POD}"],
        "timestamp": SEEN,
        "attributes": EventAttributes(**inner),
    }
    return EventResponse(
        id="AAAAAZK3dE2xyzAAAAAAAAAYAAAAAEFaSzNkRTJ4QUFDbHg",
        type=EventType.EVENT,
        attributes=EventResponseAttributes(**(values | attributes)),
    )


@dataclass
class _EventsApi:
    """Stands in for EventsApi: records the clients and bodies, then answers or raises."""

    response: EventsListResponse | Exception = field(default_factory=_response)
    clients: list[Any] = field(default_factory=list)
    bodies: list[Any] = field(default_factory=list)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> Self:
        stub = self

        class EventsApi:
            def __init__(self, api_client):
                stub.clients.append(api_client)

            async def search_events(self, *, body):
                stub.bodies.append(body)
                if isinstance(stub.response, Exception):
                    raise stub.response
                return stub.response

        monkeypatch.setattr(real, "EventsApi", EventsApi)
        return self


@pytest.fixture
def closes(monkeypatch):
    """Record each close of an aiosonic client, the transport of AsyncApiClient."""
    calls = []

    async def aclose(self):
        calls.append(self)

    monkeypatch.setattr(aiosonic.HTTPClient, "aclose", aclose)
    return calls


async def test_real_adapter_builds_request(monkeypatch, closes):
    # The client reads DD_SITE itself; the adapter uses the settings' site.
    monkeypatch.setenv("DD_SITE", "us5.datadoghq.com")
    api = _EventsApi(_response(_event_response())).install(monkeypatch)

    events = await RealDatadog(_settings()).search_events(QUERY, START, END, 50)

    (api_client,) = api.clients
    assert isinstance(api_client, AsyncApiClient)
    configuration = api_client.configuration
    assert configuration.enable_retry is True
    assert configuration.server_variables["site"] == "datadoghq.eu"
    assert configuration.api_key == {"apiKeyAuth": API_KEY, "appKeyAuth": APP_KEY}
    assert api.bodies == [
        EventsListRequest(
            filter=EventsQueryFilter(query=QUERY, _from=START_MS, to=END_MS),
            page=EventsRequestPage(limit=50),
            sort=EventsSort.TIMESTAMP_DESCENDING,
        )
    ]
    # The client of the call is closed when the call ends.
    assert len(closes) == 1

    assert events == [
        DdEvent(
            id="AAAAAZK3dE2xyzAAAAAAAAAYAAAAAEFaSzNkRTJ4QUFDbHg",
            title=f"Events from the {POD} Pod",
            text=f"BackOff: Back-off restarting failed container orders in pod {POD}",
            timestamp=SEEN,
            tags=["kube_namespace:shop", f"pod_name:{POD}"],
            source=None,
        )
    ]


async def test_real_adapter_maps_sparse_events_and_caps_the_page(monkeypatch, closes):
    sparse = EventResponse(
        attributes=EventResponseAttributes(
            attributes=EventAttributes(timestamp=1793613871120), tags=["source:github"]
        )
    )
    api = _EventsApi(
        _response(
            _event_response(inner={"source_type_name": "kubernetes"}),
            sparse,
            # No timestamp at all: the event cannot be placed in time, so it is left out.
            EventResponse(id="untimed", attributes=EventResponseAttributes(message="?")),
        )
    ).install(monkeypatch)

    events = await RealDatadog(_settings()).search_events(QUERY, START, END, 5000)

    # The API returns at most 1000 events a page.
    assert api.bodies[0].page == EventsRequestPage(limit=1000)
    assert events[0].title == "" and events[0].source == "kubernetes"
    assert events[1] == DdEvent(
        id="",
        title="",
        text="",
        timestamp=SEEN,
        tags=["source:github"],
        source="github",
    )
    assert len(events) == 2


async def test_real_adapter_errors_go_up_and_close_the_client(monkeypatch, closes):
    # A 403 (a key without events_read) is not transient: the graph does not retry it.
    forbidden = ForbiddenException(status=403, reason="Forbidden")  # type: ignore[no-untyped-call]
    api = _EventsApi(forbidden).install(monkeypatch)

    with pytest.raises(ForbiddenException) as exc_info:
        await RealDatadog(_settings()).search_events(QUERY, START, END, 50)
    assert not is_transient(exc_info.value)
    # AsyncApiClient's own __aexit__ skips the close when an exception is in flight.
    assert len(closes) == 1
    assert len(api.bodies) == 1


async def test_real_adapter_needs_both_keys(monkeypatch, closes):
    api = _EventsApi().install(monkeypatch)

    with pytest.raises(ValueError, match="DD_APP_KEY") as exc_info:
        await RealDatadog(_settings(dd_app_key=None)).search_events(QUERY, START, END, 50)
    assert "DD_API_KEY" not in str(exc_info.value)
    with pytest.raises(ValueError, match="DD_API_KEY and DD_APP_KEY"):
        await RealDatadog(_settings(dd_api_key=None, dd_app_key=None)).search_events(
            QUERY, START, END, 50
        )
    assert api.clients == []


def test_real_mode_builds_real_datadog_without_network(monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("building the adapter must not open a client or a connection")

    monkeypatch.setattr(real, "AsyncApiClient", no_network)
    monkeypatch.setattr(real, "EventsApi", no_network)
    monkeypatch.setattr(socket, "getaddrinfo", no_network)
    monkeypatch.setattr(socket.socket, "connect", no_network)

    assert isinstance(_build_datadog(_settings(), None, NO_CALL_LOG), RealDatadog)
    # Without keys too: the adapter checks them when it is called.
    assert isinstance(_build_datadog(_settings(dd_api_key=None), None, NO_CALL_LOG), RealDatadog)
    fixture = load_fixture("python-keyerror")
    assert isinstance(_build_datadog(load_settings(), fixture, NO_CALL_LOG), FakeDatadog)
    with pytest.raises(ValueError, match="fixture"):
        _build_datadog(load_settings(), None, NO_CALL_LOG)

    # Real mode builds every adapter it selects, without a fixture.
    assert isinstance(build_adapters(_settings(), fixture_id=None).datadog, RealDatadog)


# The transport tests: aiosonic answers instead of Datadog.

RESPONSE = {
    "data": [
        {
            "id": "AAAAAZK3dE2xyzAAAAAAAAAYAAAAAEFaSzNkRTJ4QUFDbHg",
            "type": "event",
            "attributes": {
                "message": "BackOff: Back-off restarting failed container orders",
                "tags": ["kube_namespace:shop", f"pod_name:{POD}"],
                "timestamp": "2026-11-02T10:04:31.120Z",
                "attributes": {
                    "title": f"Events from the {POD} Pod",
                    "source_type_name": "kubernetes",
                    "status": "warning",
                    "priority": "normal",
                    "timestamp": 1793613871120,
                },
            },
        }
    ],
    "meta": {"page": {"after": "eyJhZnRlciI6ICJ4In0="}, "status": "done", "elapsed": 12},
}


@dataclass
class _HttpResponse:
    """What aiosonic's HTTPClient.request returns, as far as the Datadog client reads it."""

    status_code: int
    body: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def response_initial(self) -> dict[str, str]:
        return {"reason": "OK" if self.status_code == 200 else "Error"}

    async def text(self) -> str:
        return json.dumps(self.body)

    async def content(self) -> bytes:
        return json.dumps(self.body).encode()


@pytest.fixture
def transport(monkeypatch, closes):
    """Swap aiosonic's request for one that records the request and answers ``statuses``.

    The test sets ``statuses``, the status of each answer in turn; the last repeats.
    Sleeps between retries are recorded, not waited for.
    """

    @dataclass
    class Transport:
        statuses: list[int] = field(default_factory=lambda: [200])
        sent: list[dict[str, Any]] = field(default_factory=list)
        sleeps: list[float] = field(default_factory=list)

    state = Transport()

    async def request(self, url, method, headers, params, data, timeouts=None):
        state.sent.append(
            {"url": url, "method": method, "headers": dict(headers), "body": json.loads(data)}
        )
        status = state.statuses[min(len(state.sent), len(state.statuses)) - 1]
        body = RESPONSE if status == 200 else {"errors": ["try again"]}
        return _HttpResponse(status, body)

    async def sleep(delay):
        state.sleeps.append(delay)

    monkeypatch.setattr(aiosonic.HTTPClient, "request", request)
    monkeypatch.setattr(asyncio, "sleep", sleep)
    return state


async def test_real_adapter_sends_the_request_to_the_site(transport, closes):
    events = await RealDatadog(_settings()).search_events(QUERY, START, END, 50)

    (sent,) = transport.sent
    assert sent["url"] == "https://api.datadoghq.eu/api/v2/events/search"
    assert sent["method"] == "POST"
    assert sent["headers"]["DD-API-KEY"] == API_KEY
    assert sent["headers"]["DD-APPLICATION-KEY"] == APP_KEY
    assert sent["body"] == {
        "filter": {"query": QUERY, "from": START_MS, "to": END_MS},
        "page": {"limit": 50},
        "sort": "-timestamp",
    }
    assert events == [
        DdEvent(
            id="AAAAAZK3dE2xyzAAAAAAAAAYAAAAAEFaSzNkRTJ4QUFDbHg",
            title=f"Events from the {POD} Pod",
            text="BackOff: Back-off restarting failed container orders",
            timestamp=SEEN,
            tags=["kube_namespace:shop", f"pod_name:{POD}"],
            source="kubernetes",
        )
    ]
    assert len(closes) == 1


async def test_real_adapter_retries_in_the_client_only(transport, closes):
    # A 503 is retried by the client (enable_retry), so the graph does not retry it again.
    transport.statuses = [503, 200]
    events = await RealDatadog(_settings()).search_events(QUERY, START, END, 50)
    assert len(events) == 1
    assert len(transport.sent) == 2
    assert transport.sleeps == [2]

    # When the client's retries run out, the error is not transient for the graph.
    transport.statuses, transport.sent, transport.sleeps = [503], [], []
    with pytest.raises(ServiceException) as unavailable:
        await RealDatadog(_settings()).search_events(QUERY, START, END, 50)
    assert len(transport.sent) == 4
    assert not is_transient(unavailable.value)

    # A 400 is not retried at all.
    transport.statuses, transport.sent = [400], []
    with pytest.raises(ApiException) as bad_request:
        await RealDatadog(_settings()).search_events(QUERY, START, END, 50)
    assert bad_request.value.status == 400
    assert len(transport.sent) == 1
    assert not is_transient(bad_request.value)


def test_page_cap_matches_the_client_model():
    assert real.MAX_PAGE_LIMIT == EventsRequestPage.validations["limit"]["inclusive_maximum"]
