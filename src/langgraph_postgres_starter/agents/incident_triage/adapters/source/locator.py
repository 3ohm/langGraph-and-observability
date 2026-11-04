"""Find the repository and commit that a target runs (spec §9.6 item 1).

``ChainLocator.locate`` tries three origins in order and returns the first that names
both a repository and a commit:

1. ``annotation``: the workload's annotations, with the keys ``APP_SOURCE_ANNOTATION_
   REPO_KEY`` and ``APP_SOURCE_ANNOTATION_COMMIT_KEY``;
2. ``image_label``: the OCI labels ``org.opencontainers.image.source`` and
   ``org.opencontainers.image.revision`` of the target container's image, read from
   ECR (``ecr.py``);
3. ``config``: the source map, ``APP_SOURCE_MAP_FILE``, a YAML mapping from
   ``<namespace>/<workload name>`` to the repository and ref::

       shop/orders:
         repo: https://github.com/example/orders
         ref: main

These values come from the cluster, so they are untrusted: an origin whose URL or
commit fails ``git.check_repo_url`` (an ``https://`` URL on a host in
``APP_GIT_ALLOWED_HOSTS``, ruling R59) or ``git.check_commit`` is skipped with a
warning, and the next origin is tried. So is an origin whose image labels cannot be
read, or whose source map is missing or malformed. A ref holds the URL's canonical
form. The map is read on every call, so a mounted ConfigMap's updates apply to the
next run.

Each match is logged as ``source.locate.found`` with its origin, URL and commit. A
URL that passes the checks holds no credentials, so no log holds a token.
"""

import asyncio
from collections.abc import Collection, Mapping
from pathlib import Path
from typing import Literal, TypedDict

import yaml
from pydantic import ConfigDict, TypeAdapter, ValidationError, with_config

from langgraph_postgres_starter.agents.incident_triage.adapters.source.ecr import (
    EcrLabelReader,
    ImageLabelError,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.git import (
    check_commit,
    check_repo_url,
)
from langgraph_postgres_starter.agents.incident_triage.adapters.source.interface import (
    SourceLocator,
    SourceRef,
)
from langgraph_postgres_starter.agents.incident_triage.schemas import Target
from langgraph_postgres_starter.logging import get_logger

_log = get_logger(__name__)

OCI_SOURCE_LABEL = "org.opencontainers.image.source"
OCI_REVISION_LABEL = "org.opencontainers.image.revision"

type _Origin = Literal["annotation", "image_label", "config"]


# One entry of the source map. A missing or unknown key is an error.
@with_config(ConfigDict(extra="forbid"))
class _MapEntry(TypedDict):
    repo: str
    ref: str


_SOURCE_MAP = TypeAdapter(dict[str, _MapEntry])


class ChainLocator(SourceLocator):
    """Finds a target's source in its annotations, its image's labels, then the source map.

    ``annotation_keys`` are the annotations' repository and commit keys, and
    ``allowed_hosts`` the hosts that a repository URL may name. Without ``ecr`` the
    image labels are not read; without ``config_map`` there is no map. Building the
    locator reads nothing.
    """

    def __init__(
        self,
        *,
        annotation_keys: tuple[str, str],
        allowed_hosts: Collection[str],
        ecr: EcrLabelReader | None,
        config_map: Path | None,
    ) -> None:
        self._repo_key, self._commit_key = annotation_keys
        self._allowed_hosts = tuple(allowed_hosts)
        self._ecr = ecr
        self._config_map = config_map

    async def locate(self, target: Target, annotations: Mapping[str, str]) -> SourceRef | None:
        ref = (
            self._from_annotations(annotations)
            or await self._from_image_labels(target)
            or await self._from_config_map(target)
        )
        if ref is None:
            _log.info("source.locate.none", namespace=target.namespace)
        else:
            _log.info(
                "source.locate.found",
                origin=ref.origin,
                repo_url=ref.repo_url,
                commit=ref.commit,
            )
        return ref

    def _from_annotations(self, annotations: Mapping[str, str]) -> SourceRef | None:
        return self._ref(
            annotations.get(self._repo_key), annotations.get(self._commit_key), "annotation"
        )

    async def _from_image_labels(self, target: Target) -> SourceRef | None:
        """The labels of the target container's image."""
        if self._ecr is None:
            return None
        image = next((i for i in target.images if i.container == target.container), None)
        if image is None:
            return None
        try:
            labels = await self._ecr.labels(image.image, image.digest)
        except ImageLabelError as exc:
            _log.warning("source.locate.labels_failed", image=image.image, error=str(exc))
            return None
        return self._ref(
            labels.get(OCI_SOURCE_LABEL), labels.get(OCI_REVISION_LABEL), "image_label"
        )

    async def _from_config_map(self, target: Target) -> SourceRef | None:
        """The source map's entry for the target's workload."""
        if self._config_map is None or target.workload is None:
            return None
        try:
            text = await asyncio.to_thread(self._config_map.read_text, encoding="utf-8")
            entries = _SOURCE_MAP.validate_python(yaml.safe_load(text) or {})
        except (OSError, UnicodeDecodeError) as exc:
            self._map_invalid(type(exc).__name__)
            return None
        except yaml.YAMLError:
            self._map_invalid("YAMLError")
            return None
        except ValidationError as exc:
            failures = exc.errors(include_url=False, include_context=False, include_input=False)
            self._map_invalid(
                "ValidationError",
                errors=[{"type": f["type"], "loc": list(f["loc"])} for f in failures],
            )
            return None
        entry = entries.get(f"{target.namespace}/{target.workload.name}")
        return None if entry is None else self._ref(entry["repo"], entry["ref"], "config")

    def _ref(self, repo_url: str | None, commit: str | None, origin: _Origin) -> SourceRef | None:
        """A SourceRef from an origin's values; None if one is missing or fails the checks."""
        if not repo_url or not commit:
            return None
        try:
            url = check_repo_url(repo_url, allowed_hosts=self._allowed_hosts)
            return SourceRef(repo_url=url, commit=check_commit(commit), origin=origin)
        except ValueError as exc:
            # The checks' messages never repeat the URL, which could hold a credential.
            _log.warning("source.locate.skipped", origin=origin, reason=str(exc))
            return None

    def _map_invalid(self, error: str, **fields: object) -> None:
        # The error's type and location only: the file's text is not logged.
        _log.warning(
            "source.locate.config_map_invalid", path=str(self._config_map), error=error, **fields
        )
