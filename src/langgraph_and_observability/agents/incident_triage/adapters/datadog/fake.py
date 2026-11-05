"""A Datadog adapter that serves an incident fixture's ``datadog.json`` (fixtures/FORMAT.md)."""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Self, TypedDict

from pydantic import ConfigDict, TypeAdapter, with_config

from langgraph_and_observability.agents.incident_triage.adapters.call_log import (
    NO_CALL_LOG,
    CallLog,
)
from langgraph_and_observability.agents.incident_triage.adapters.datadog.interface import (
    DatadogAdapter,
    DdEvent,
)


# The shape of datadog.json. A missing or unknown key is an error, also in the events.
@with_config(ConfigDict(extra="forbid"))
class _DatadogJson(TypedDict):
    events: list[DdEvent]


_DATADOG_JSON = TypeAdapter(_DatadogJson)


@dataclass(frozen=True)
class FakeDatadog(DatadogAdapter):
    """Serves fixed events, after a delay.

    Each search writes ``datadog.search_events:start`` to ``calls``, sleeps
    ``delay_s`` seconds (``APP_FAKE_DATADOG_DELAY_S``, so a test can stop a worker
    during this step), then writes ``datadog.search_events`` and returns.
    """

    events: list[DdEvent] = field(default_factory=list)
    delay_s: float = 0.0
    calls: CallLog = NO_CALL_LOG

    @classmethod
    def from_fixture(
        cls, datadog: Mapping[str, Any], *, delay_s: float = 0.0, calls: CallLog = NO_CALL_LOG
    ) -> Self:
        """Build the fake from a ``datadog.json`` object. Raises pydantic's ValidationError."""
        return cls(
            events=_DATADOG_JSON.validate_python(datadog)["events"], delay_s=delay_s, calls=calls
        )

    async def search_events(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> list[DdEvent]:
        """The first ``limit`` events, whatever the query and time window.

        The window follows the incident's ``fired_at``. A fixture incident has none, so
        its window is around the time the message arrived and would not match the
        fixture's fixed timestamps.
        """
        self.calls.record("datadog.search_events:start")
        await asyncio.sleep(self.delay_s)
        self.calls.record("datadog.search_events")
        return self.events[:limit]
