"""The Datadog adapter's interface (spec §9.4): event search.

``FakeDatadog`` (``fake.py``) serves an incident fixture.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class DdEvent:
    """A Datadog event. ``source`` is the integration that sent it, if known."""

    id: str
    title: str
    text: str
    timestamp: datetime
    tags: list[str]
    source: str | None


class DatadogAdapter(Protocol):
    """Searches Datadog events."""

    async def search_events(
        self, query: str, start: datetime, end: datetime, limit: int
    ) -> list[DdEvent]:
        """At most ``limit`` events that match ``query`` between ``start`` and ``end``."""
        ...
