"""Loads the incident fixtures that the simulated adapters serve (spec §10.2).

A fixtures directory holds ``incidents/``, ``apps/`` and ``recorded/``. FORMAT.md, next
to this module, describes each file. ``load_fixture`` returns an incident's files as
plain JSON objects; the fakes in ``adapters/`` check and convert them.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# This package's fixtures directory: the default root, and the default of
# Settings.fixtures_dir (APP_FIXTURES_DIR).
FIXTURES_DIR = Path(__file__).parent


@dataclass(frozen=True)
class Fixture:
    """One incident fixture: the JSON objects of ``incidents/<id>/``.

    ``source`` is None when the fixture has no ``source.json`` (an incident with no
    app, such as an infrastructure failure).
    """

    id: str
    incident: dict[str, Any]
    k8s: dict[str, Any]
    datadog: dict[str, Any]
    expected: dict[str, Any]
    source: dict[str, Any] | None


def load_fixture(fixture_id: str, root: Path | None = None) -> Fixture:
    """Load the incident ``fixture_id`` from ``root/incidents`` (default: ``FIXTURES_DIR``).

    ``fixture_id`` may name a folder below ``incidents/``, such as
    ``_test/invalid-json``. Raises ValueError when it names a path outside
    ``incidents/``, FileNotFoundError (naming the path) when the folder or a required
    file is missing, and ValueError (naming the file) when a file is not a JSON object.
    """
    incidents = (FIXTURES_DIR if root is None else root) / "incidents"
    folder = path_inside(incidents, fixture_id)
    if not folder.is_dir():
        raise FileNotFoundError(f"no incident fixture {fixture_id!r}: {folder} is not a folder")
    return Fixture(
        id=fixture_id,
        incident=_read(folder / "incident.json"),
        k8s=_read(folder / "k8s.json"),
        datadog=_read(folder / "datadog.json"),
        expected=_read(folder / "expected.json"),
        source=_read_optional(folder / "source.json"),
    )


def list_fixtures(root: Path | None = None) -> list[str]:
    """Return the IDs of the incidents in ``root/incidents``, sorted.

    Folders whose name starts with ``_`` hold test-only fixtures and are left out.
    """
    incidents = (FIXTURES_DIR if root is None else root) / "incidents"
    return sorted(
        entry.name
        for entry in incidents.iterdir()
        if entry.is_dir() and not entry.name.startswith("_")
    )


def path_inside(base: Path, relative: str) -> Path:
    """Return ``base / relative``, a path strictly inside ``base``.

    Raises ValueError when ``relative`` is empty or absolute, or when ``..`` (or a
    symbolic link) leads out of ``base``. IDs from queue messages name fixture files,
    so they must not reach other files.
    """
    path = base / relative
    resolved, resolved_base = path.resolve(), base.resolve()
    if resolved == resolved_base or not resolved.is_relative_to(resolved_base):
        raise ValueError(f"{relative!r} does not name a path inside {base}")
    return path


def _read(path: Path) -> dict[str, Any]:
    """Return the JSON object in ``path``. Every error names the file."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise FileNotFoundError(f"the fixture file {path} is missing") from None
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"the fixture file {path} is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"the fixture file {path} must hold a JSON object")
    return value


def _read_optional(path: Path) -> dict[str, Any] | None:
    """Like ``_read``, but return None when the file does not exist."""
    try:
        return _read(path)
    except FileNotFoundError:
        return None
