"""The source adapters' interfaces (spec §9.6, "Finding the source").

A ``SourceLocator`` finds the repository and commit that a target runs; a
``SourceProvider`` checks that commit out into a local folder.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from langgraph_postgres_starter.agents.incident_triage.schemas import Target


@dataclass(frozen=True)
class SourceRef:
    """A repository and commit, and where the locator found them.

    ``origin`` is ``annotation`` (workload annotations), ``image_label`` (OCI image
    labels), ``config`` (``APP_SOURCE_MAP_FILE``) or ``fixture`` (a fixture's
    ``source.json``).
    """

    repo_url: str
    commit: str
    origin: Literal["annotation", "image_label", "config", "fixture"]


class SourceLocator(Protocol):
    """Finds the source of a target's code."""

    async def locate(self, target: Target, annotations: Mapping[str, str]) -> SourceRef | None:
        """The target's repository and commit, or None if no source names them.

        ``annotations`` are the annotations of the target's workload.
        """
        ...


class SourceProvider(Protocol):
    """Checks out source code."""

    async def checkout(self, ref: SourceRef) -> Path:
        """The local folder that holds ``ref``'s repository at ``ref.commit``."""
        ...
