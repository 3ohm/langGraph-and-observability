"""``scripts/sync_dataset.py``: the eval cases as a LangSmith dataset (spec §12.3).

A sync is idempotent by fixture ID. The client is a fake that keeps the datasets and
examples in memory, so no test reaches LangSmith.

The script imports the example agent, so a fork without it skips this module.
"""

import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, cast

import langsmith
import pytest

sync_dataset = pytest.importorskip("scripts.sync_dataset")
load_cases = pytest.importorskip(
    "langgraph_postgres_starter.agents.incident_triage.evals.dataset"
).load_cases


@dataclass
class FakeExample:
    id: uuid.UUID
    dataset_id: uuid.UUID
    inputs: dict[str, Any]
    outputs: dict[str, Any] | None
    metadata: dict[str, Any] | None


@dataclass
class FakeClient:
    """The ``langsmith.Client`` methods that the script calls, in memory."""

    datasets: dict[str, SimpleNamespace] = field(default_factory=dict)
    examples: list[FakeExample] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)

    def has_dataset(self, *, dataset_name: str) -> bool:
        return dataset_name in self.datasets

    def read_dataset(self, *, dataset_name: str) -> SimpleNamespace:
        return self.datasets[dataset_name]

    def create_dataset(self, dataset_name: str, *, description: str) -> SimpleNamespace:
        self.calls.append("create_dataset")
        dataset = SimpleNamespace(id=uuid.uuid4(), name=dataset_name)
        self.datasets[dataset_name] = dataset
        return dataset

    def list_examples(self, *, dataset_id: uuid.UUID) -> list[FakeExample]:
        return [example for example in self.examples if example.dataset_id == dataset_id]

    def create_example(self, *, inputs, outputs, metadata, dataset_id) -> FakeExample:
        self.calls.append("create_example")
        # LangSmith adds a split to the metadata.
        example = FakeExample(
            uuid.uuid4(), dataset_id, inputs, outputs, {**metadata, "dataset_split": ["base"]}
        )
        self.examples.append(example)
        return example

    def update_example(self, example_id, *, inputs, outputs, metadata, dataset_id) -> None:
        self.calls.append("update_example")
        [example] = [example for example in self.examples if example.id == example_id]
        example.inputs, example.outputs, example.metadata = inputs, outputs, metadata


def _sync(client: FakeClient, name: str = "incident-triage-v1") -> Any:
    return sync_dataset.sync(cast(Any, client), name, load_cases())


def _by_fixture(client: FakeClient) -> dict[str, FakeExample]:
    return {(example.metadata or {})["fixture_id"]: example for example in client.examples}


def test_first_sync_creates_the_dataset_and_one_example_per_case():
    client = FakeClient()

    counts = _sync(client)

    assert (counts.created, counts.updated, counts.unchanged) == (16, 0, 0)
    assert list(client.datasets) == ["incident-triage-v1"]
    examples = _by_fixture(client)
    assert sorted(examples) == [case.fixture_id for case in load_cases()]
    example = examples["python-keyerror"]
    assert example.inputs["incident"]["raw"]["fixture_id"] == "python-keyerror"
    assert example.outputs is not None and example.outputs["category"] == "application"


def test_sync_is_idempotent_by_fixture_id():
    client = FakeClient()
    _sync(client)
    client.calls.clear()

    counts = _sync(client)

    assert (counts.created, counts.updated, counts.unchanged) == (0, 0, 16)
    assert client.calls == []
    assert len(client.examples) == 16


def test_sync_updates_a_changed_case_and_keeps_other_examples():
    client = FakeClient()
    _sync(client)
    dataset = client.datasets["incident-triage-v1"]
    examples = _by_fixture(client)
    stale = examples["infra-oomkilled"]
    stale.outputs = {"category": "application", "code": None, "facts": []}
    other = FakeExample(uuid.uuid4(), dataset.id, {"note": "added by hand"}, None, None)
    client.examples.append(other)
    client.calls.clear()

    counts = _sync(client)

    assert (counts.created, counts.updated, counts.unchanged) == (0, 1, 15)
    assert client.calls == ["update_example"]
    assert stale.outputs is not None and stale.outputs["category"] == "infrastructure"
    assert stale.metadata is not None and stale.metadata["fixture_id"] == "infra-oomkilled"
    assert other in client.examples


def test_a_deleted_example_is_created_again():
    client = FakeClient()
    _sync(client)
    client.examples = [
        e for e in client.examples if (e.metadata or {}).get("fixture_id") != "go-nil-map"
    ]

    counts = _sync(client)

    assert (counts.created, counts.updated, counts.unchanged) == (1, 0, 15)


def test_main_without_an_api_key_exits_with_a_clear_message(monkeypatch, capsys):
    def no_client(*args, **kwargs):
        raise AssertionError("the script built a LangSmith client without a key")

    monkeypatch.setattr(langsmith, "Client", no_client)

    with pytest.raises(SystemExit) as exit_info:
        sync_dataset.main([])

    assert exit_info.value.code == 1
    assert "LANGSMITH_API_KEY is not set" in capsys.readouterr().err


def test_main_syncs_the_named_dataset(monkeypatch, capsys):
    client = FakeClient()
    monkeypatch.setenv("LANGSMITH_API_KEY", "test-key")
    monkeypatch.setattr(langsmith, "Client", lambda: client)

    assert sync_dataset.main(["--dataset", "triage-evals"]) == 0

    assert list(client.datasets) == ["triage-evals"]
    assert capsys.readouterr().out.strip() == (
        "dataset 'triage-evals': 16 created, 0 updated, 0 unchanged"
    )


def test_main_defaults_to_incident_triage_v1(monkeypatch):
    client = FakeClient()
    monkeypatch.setenv("LANGSMITH_API_KEY", "test-key")
    monkeypatch.setattr(langsmith, "Client", lambda: client)

    sync_dataset.main([])

    assert list(client.datasets) == ["incident-triage-v1"]


def test_main_reports_a_langsmith_error(monkeypatch, capsys):
    class DownClient(FakeClient):
        def has_dataset(self, *, dataset_name: str) -> bool:
            raise langsmith.utils.LangSmithConnectionError("LangSmith is down")

    monkeypatch.setenv("LANGSMITH_API_KEY", "test-key")
    monkeypatch.setattr(langsmith, "Client", DownClient)

    with pytest.raises(SystemExit) as exit_info:
        sync_dataset.main([])

    assert exit_info.value.code == 1
    assert "the dataset was not synced: LangSmith is down" in capsys.readouterr().err
