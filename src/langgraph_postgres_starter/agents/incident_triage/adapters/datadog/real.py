"""The real Datadog adapter, through datadog-api-client's async client (spec §9.4, §9.10).

``search_events`` calls ``EventsApi.search_events`` (``POST /api/v2/events/search``)
with an ``EventsListRequest``: the filter (query, ``from`` and ``to`` in milliseconds
since the epoch), one page of at most ``limit`` events, newest first.

The settings are the one source of the site and the keys (``DD_SITE``, ``DD_API_KEY``,
``DD_APP_KEY``). The client's ``Configuration`` would also read them from the
environment, and its ``DD_SITE`` would win over an explicit site, so the adapter sets
the site after it builds the configuration. The keys stay ``SecretStr`` until a call
builds its configuration, and a call without both keys raises ValueError before it
opens a client. Building the adapter touches no network.

Each call opens an ``AsyncApiClient``, whose HTTP transport is aiosonic (the client's
``async`` extra), and closes it before it returns, also when the call fails.

Retries: the client retries a 429 and a 5xx itself (``enable_retry=True``), so every
``ApiException`` that comes out has had its retries; ``policies.is_transient`` does not
accept it, and the graph does not retry it again. The client does not retry transport
errors. Of those, the graph's retry policy retries only what is a stdlib
``ConnectionError`` or ``TimeoutError``, such as a refused connection; aiosonic's own
timeouts and dropped connections (``aiosonic.exceptions.AiosonicError``) are neither,
so no layer retries them.
"""

from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from datadog_api_client import AsyncApiClient, Configuration
from datadog_api_client.v2.api.events_api import EventsApi
from datadog_api_client.v2.model.events_list_request import EventsListRequest
from datadog_api_client.v2.model.events_list_response import EventsListResponse
from datadog_api_client.v2.model.events_query_filter import EventsQueryFilter
from datadog_api_client.v2.model.events_request_page import EventsRequestPage
from datadog_api_client.v2.model.events_sort import EventsSort

from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.interface import (
    DatadogAdapter,
    DdEvent,
)
from langgraph_postgres_starter.settings import Settings

# The most events that one page of the search returns (EventsRequestPage's limit).
MAX_PAGE_LIMIT = 1000

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class RealDatadog(DatadogAdapter):
    """Searches the events of the Datadog site and organization that the settings name."""

    def __init__(self, settings: Settings) -> None:
        self._site = settings.dd_site
        self._api_key = settings.dd_api_key
        self._app_key = settings.dd_app_key

    async def search_events(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> list[DdEvent]:
        """The newest events, at most ``limit`` (and at most one page), newest first.

        An event without a timestamp is left out; the API always sends one. A time
        without a zone is taken to be UTC.
        """
        configuration = self._configuration()
        body = EventsListRequest(
            filter=EventsQueryFilter(query=query, _from=_ms(start), to=_ms(end)),
            page=EventsRequestPage(limit=min(limit, MAX_PAGE_LIMIT)),
            sort=EventsSort.TIMESTAMP_DESCENDING,
        )
        async with _open(configuration) as api_client:
            # The client's methods are typed for its sync client; the async client
            # returns an awaitable.
            api = EventsApi(api_client)  # type: ignore[no-untyped-call]
            response = await cast(Awaitable[EventsListResponse], api.search_events(body=body))
        events = (_event(item) for item in getattr(response, "data", None) or [])
        return [event for event in events if event is not None]

    def _configuration(self) -> Configuration:
        """The client configuration of one call. Raises ValueError without both keys."""
        api_key, app_key = self._api_key, self._app_key
        if api_key is None or app_key is None:
            missing = [
                name
                for name, key in (("DD_API_KEY", api_key), ("DD_APP_KEY", app_key))
                if key is None
            ]
            raise ValueError(
                f"the real Datadog adapter needs {' and '.join(missing)} to be set "
                "(or use APP_ADAPTERS=simulated)"
            )
        configuration = Configuration(  # type: ignore[no-untyped-call]
            api_key={
                "apiKeyAuth": api_key.get_secret_value(),
                "appKeyAuth": app_key.get_secret_value(),
            },
            enable_retry=True,
        )
        # After the constructor, which reads DD_SITE from the environment over this value.
        configuration.server_variables["site"] = self._site
        return configuration


@asynccontextmanager
async def _open(configuration: Configuration) -> AsyncIterator[AsyncApiClient]:
    """An API client for one call, closed when the call ends.

    ``AsyncApiClient.__aexit__`` closes its HTTP client only when no exception is in
    flight, so this calls it without one, in every case.
    """
    api_client = AsyncApiClient(configuration)
    try:
        yield api_client
    finally:
        await api_client.__aexit__(None, None, None)  # type: ignore[no-untyped-call]


def _ms(value: datetime) -> str:
    """``value`` in milliseconds since the epoch, as the search filter takes it."""
    aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return str((aware - _EPOCH) // timedelta(milliseconds=1))


def _event(item: Any) -> DdEvent | None:
    """One event of the response, or None when it has no timestamp.

    The client's models raise AttributeError for a field that the response left out,
    so every field is read with a default. The title and the source are in the
    event's own ``attributes``; the source falls back to a ``source:`` tag.
    """
    attributes = getattr(item, "attributes", None)
    inner = getattr(attributes, "attributes", None)
    timestamp = _timestamp(attributes, inner)
    if timestamp is None:
        return None
    tags = [str(tag) for tag in getattr(attributes, "tags", None) or []]
    source = getattr(inner, "source_type_name", None) or _tag_value(tags, "source")
    return DdEvent(
        id=str(getattr(item, "id", None) or ""),
        title=str(getattr(inner, "title", None) or ""),
        text=str(getattr(attributes, "message", None) or ""),
        timestamp=timestamp,
        tags=tags,
        source=None if source is None else str(source),
    )


def _timestamp(attributes: Any, inner: Any) -> datetime | None:
    """The event's time: its ``timestamp``, else its own attributes' one (milliseconds)."""
    value = getattr(attributes, "timestamp", None)
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    ms = getattr(inner, "timestamp", None)
    if isinstance(ms, int):
        return _EPOCH + timedelta(milliseconds=ms)
    return None


def _tag_value(tags: list[str], key: str) -> str | None:
    """The value of the first tag ``key:<value>``, or None."""
    prefix = f"{key}:"
    return next((tag[len(prefix) :] for tag in tags if tag.startswith(prefix)), None)
