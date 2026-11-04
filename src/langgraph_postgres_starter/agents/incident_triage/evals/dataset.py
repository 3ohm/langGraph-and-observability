"""The eval dataset: one case per incident fixture (spec §12.3).

A case is an incident in ``fixtures/incidents/`` and its ``expected.json``
(``fixtures/FORMAT.md``):

- ``category``: the expected category;
- ``code``: the expected code location (``path``, ``start_line``, ``end_line``,
  ``symbol``) of an application case, or null for a case that is not one;
- ``facts``: the key facts that a correct root cause names.

There is one case per sample app (spec §13.10) and four infrastructure cases. The
test-only fixtures below ``incidents/_test/`` are not cases.
"""

from dataclasses import dataclass
from typing import Any

from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import (
    list_fixtures,
    load_fixture,
)


@dataclass(frozen=True)
class EvalCase:
    """One case: the incident fixture ``fixture_id`` and its ``expected.json``."""

    fixture_id: str
    expected: dict[str, Any]


def load_cases() -> list[EvalCase]:
    """Every incident fixture as a case, sorted by fixture ID (``list_fixtures``)."""
    return [
        EvalCase(fixture_id=fixture_id, expected=load_fixture(fixture_id).expected)
        for fixture_id in list_fixtures()
    ]
