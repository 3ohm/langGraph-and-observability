"""Upload the Incident Triage eval cases to a LangSmith dataset (spec §12.3).

Usage::

    LANGSMITH_API_KEY=... uv run scripts/sync_dataset.py [--dataset incident-triage-v1]

Each case (``evals/dataset.py``: one per incident fixture) is one example:

- inputs: ``{"incident": <the fixture's incident.json>}``;
- outputs: the case's ``expected.json``, the reference;
- metadata: ``{"fixture_id": <the fixture ID>}``.

A sync is idempotent by fixture ID. It creates the dataset when it is missing, an
example for each case that has none, and updates an example whose inputs or outputs
differ from its case's. Examples without a case's fixture ID stay as they are. It prints
how many examples it created, updated and left unchanged.

langsmith's client reads ``LANGSMITH_API_KEY``, and ``LANGSMITH_ENDPOINT`` for another
region or a self-hosted LangSmith. Without a key the script exits 1 and says so; a
LangSmith error exits 1 too, with its message on stderr.
"""

import argparse
import os
from collections.abc import Sequence
from dataclasses import dataclass

import langsmith
from langsmith.schemas import Example
from langsmith.utils import LangSmithError

from langgraph_postgres_starter.agents.incident_triage.evals.dataset import EvalCase, load_cases
from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import load_fixture

DEFAULT_DATASET = "incident-triage-v1"
DESCRIPTION = (
    "Incident Triage eval cases: one incident fixture each, with its expected category, "
    "code location and key facts (langgraph-postgres-starter, scripts/sync_dataset.py)."
)
# The variables that langsmith's client reads its API key from.
_API_KEYS = ("LANGSMITH_API_KEY", "LANGCHAIN_API_KEY")


@dataclass(frozen=True)
class SyncCounts:
    """How many examples a sync created, updated and left unchanged."""

    created: int
    updated: int
    unchanged: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sync_dataset.py",
        description="Upload the Incident Triage eval cases to a LangSmith dataset.",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        metavar="NAME",
        help="the dataset's name (default: %(default)s)",
    )
    return parser


def sync(client: langsmith.Client, dataset_name: str, cases: Sequence[EvalCase]) -> SyncCounts:
    """Make the dataset ``dataset_name`` hold one example per case, keyed by fixture ID.

    The module docstring gives each example's fields. When several examples have the
    same fixture ID, the first one listed is the case's. LangSmith's errors are raised
    as they are.
    """
    if client.has_dataset(dataset_name=dataset_name):
        dataset = client.read_dataset(dataset_name=dataset_name)
    else:
        dataset = client.create_dataset(dataset_name, description=DESCRIPTION)
    existing: dict[str, Example] = {}
    for example in client.list_examples(dataset_id=dataset.id):
        fixture_id = (example.metadata or {}).get("fixture_id")
        if isinstance(fixture_id, str):
            existing.setdefault(fixture_id, example)
    created = updated = unchanged = 0
    for case in cases:
        inputs = {"incident": load_fixture(case.fixture_id).incident}
        metadata = {"fixture_id": case.fixture_id}
        found = existing.get(case.fixture_id)
        if found is None:
            client.create_example(
                inputs=inputs, outputs=case.expected, metadata=metadata, dataset_id=dataset.id
            )
            created += 1
        elif found.inputs == inputs and found.outputs == case.expected:
            unchanged += 1
        else:
            client.update_example(
                found.id,
                inputs=inputs,
                outputs=case.expected,
                metadata={**(found.metadata or {}), **metadata},
                dataset_id=dataset.id,
            )
            updated += 1
    return SyncCounts(created=created, updated=updated, unchanged=unchanged)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not any(os.environ.get(name, "").strip() for name in _API_KEYS):
        parser.exit(
            1,
            f"{parser.prog}: error: LANGSMITH_API_KEY is not set. The script uploads the eval "
            "cases to LangSmith, which needs an API key: set LANGSMITH_API_KEY (and "
            "LANGSMITH_ENDPOINT outside LangSmith's US region).\n",
        )
    try:
        counts = sync(langsmith.Client(), args.dataset, load_cases())
    except LangSmithError as exc:
        parser.exit(1, f"{parser.prog}: error: the dataset was not synced: {exc}\n")
    print(
        f"dataset {args.dataset!r}: {counts.created} created, {counts.updated} updated, "
        f"{counts.unchanged} unchanged"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
