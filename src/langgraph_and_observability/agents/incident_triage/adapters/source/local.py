"""Source adapters for simulated mode: the fixture apps (spec §9.6, "a local-folder adapter").

A fixture's ``source.json`` names its app, ``<fixtures>/apps/<app>``. The locator
returns it as ``fixture://<app>``, and the provider maps that URL back to the folder.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self, TypedDict

from pydantic import ConfigDict, TypeAdapter, with_config

from langgraph_and_observability.agents.incident_triage.adapters.call_log import (
    NO_CALL_LOG,
    CallLog,
)
from langgraph_and_observability.agents.incident_triage.adapters.source.interface import (
    SourceLocator,
    SourceProvider,
    SourceRef,
)
from langgraph_and_observability.agents.incident_triage.fixtures.loader import path_inside
from langgraph_and_observability.agents.incident_triage.schemas import Target

FIXTURE_SCHEME = "fixture://"


# The shape of source.json. A missing or unknown key is an error.
@with_config(ConfigDict(extra="forbid"))
class _SourceJson(TypedDict):
    app: str
    commit: str


_SOURCE_JSON = TypeAdapter(_SourceJson)


@dataclass(frozen=True)
class FixtureLocator(SourceLocator):
    """Returns a fixture's source, ``ref``, for every target.

    Each call writes ``locator.locate``.
    """

    ref: SourceRef | None
    calls: CallLog = NO_CALL_LOG

    @classmethod
    def from_fixture(cls, source: Mapping[str, Any] | None, calls: CallLog = NO_CALL_LOG) -> Self:
        """Build the locator from a ``source.json`` object, or None for a fixture without one.

        Raises pydantic's ValidationError.
        """
        if source is None:
            return cls(ref=None, calls=calls)
        data = _SOURCE_JSON.validate_python(source)
        ref = SourceRef(
            repo_url=FIXTURE_SCHEME + data["app"], commit=data["commit"], origin="fixture"
        )
        return cls(ref=ref, calls=calls)

    async def locate(self, target: Target, annotations: Mapping[str, str]) -> SourceRef | None:
        self.calls.record("locator.locate")
        return self.ref


@dataclass(frozen=True)
class LocalSourceProvider(SourceProvider):
    """Checks out ``fixture://<app>`` sources: the folder ``apps_dir/<app>``, as it is.

    Fixture apps have no history, so the commit is not used. Each call writes
    ``source.checkout``.
    """

    apps_dir: Path
    calls: CallLog = NO_CALL_LOG

    async def checkout(self, ref: SourceRef) -> Path:
        """Return the app's folder.

        Raises ValueError for a URL that is not ``fixture://<app>`` or that leads out of
        ``apps_dir``, and FileNotFoundError when the app's folder does not exist.
        """
        self.calls.record("source.checkout")
        if not ref.repo_url.startswith(FIXTURE_SCHEME):
            raise ValueError(
                f"the local source provider serves only {FIXTURE_SCHEME}<app> sources, "
                f"not {ref.repo_url!r}"
            )
        folder = path_inside(self.apps_dir, ref.repo_url.removeprefix(FIXTURE_SCHEME))
        if not folder.is_dir():
            raise FileNotFoundError(f"no fixture app at {folder}")
        return folder
