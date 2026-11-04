"""LangSmith tracing, content modes and run metadata (spec §9.7, §12.1; P14, P15).

No test uses the network. Each real ``langsmith.Client`` that a test builds gets a
session that fails any request and no background batching thread, so it can send
nothing. It is closed at the end of the test.
"""

import copy
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import langsmith
import pytest
import requests
from langchain_core.callbacks import CallbackManager
from langchain_core.tracers.langchain import LangChainTracer
from langchain_core.tracers.langchain import get_client as langchain_get_client
from langsmith import utils as ls_utils
from langsmith.run_helpers import get_current_run_tree, get_tracing_context, tracing_context
from langsmith.run_trees import get_cached_client

from langgraph_postgres_starter import __version__
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.logging import configure_logging
from langgraph_postgres_starter.observability.langsmith import (
    FLUSH_TIMEOUT_S,
    close_tracing,
    configure_tracing,
    content_hooks,
    trace_llm,
)
from langgraph_postgres_starter.observability.metadata import run_metadata
from langgraph_postgres_starter.settings import load_settings
from langgraph_postgres_starter.worker.runs import RunRow, RunStatus

# Nothing listens on port 1: a request that got past NoNetworkSession would fail too.
NO_NETWORK = "http://127.0.0.1:1"
# Bound at import: tests patch the attributes on the langsmith module.
REAL_CLIENT = langsmith.Client
RESET = langsmith.configure


class NoNetworkSession(requests.Session):
    """A requests session that fails every request."""

    def request(self, *args, **kwargs):
        raise AssertionError("a unit test tried to reach the network")


def clear_env_cache() -> None:
    """Make langsmith read its variables afresh. It caches them (get_env_var, lru_cache)."""
    cast(Any, ls_utils.get_env_var).cache_clear()


def offline(**kwargs) -> dict[str, Any]:
    """``kwargs`` plus the arguments that keep a real client offline."""
    return {**kwargs, "session": NoNetworkSession(), "auto_batch_tracing": False}


@pytest.fixture(autouse=True)
def _isolate_langsmith(monkeypatch):
    """Hide the developer's LangSmith variables; reset langsmith's global defaults after.

    The prefixes cover the four names that turn tracing on (LANGSMITH_TRACING,
    LANGCHAIN_TRACING_V2, LANGSMITH_API_KEY, LANGCHAIN_API_KEY), and the project,
    endpoint and hide variables.
    """
    for name in list(os.environ):
        if name.upper().startswith(("LANGSMITH_", "LANGCHAIN_")):
            monkeypatch.delenv(name)
    clear_env_cache()
    yield
    RESET(client=None, project_name=None)
    clear_env_cache()


@pytest.fixture
def tracing_env(monkeypatch):
    """Tracing on, with a fake key and an endpoint that nothing listens on."""
    monkeypatch.setenv("LANGSMITH_TRACING", "true")
    monkeypatch.setenv("LANGSMITH_API_KEY", "test-key")
    monkeypatch.setenv("LANGSMITH_ENDPOINT", NO_NETWORK)


@pytest.fixture
def clients(monkeypatch):
    """Each langsmith.Client that the code builds, with the code's arguments; closed after.

    Each one is a real client, built offline (``offline``).
    """
    built = []

    def build(**kwargs):
        client = REAL_CLIENT(**offline(**kwargs))
        built.append((client, kwargs))
        return client

    monkeypatch.setattr(langsmith, "Client", build)
    yield built
    for client, _ in built:
        client.close(timeout=0)


@pytest.fixture
def offline_client():
    """A real client that cannot send, for ``tracing_context(enabled="local", client=...)``.

    "local" builds run trees and never posts them. traceable still reads each run's
    client (for OpenTelemetry), and without one in the context it would build langsmith's
    default client.
    """
    client = REAL_CLIENT(**offline(api_url=NO_NETWORK, api_key="test-key"))
    yield client
    client.close(timeout=0)


# --- configure_tracing: when tracing is on -------------------------------------------


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"LANGSMITH_TRACING": "true"},
        {"LANGCHAIN_TRACING_V2": "true"},
        {"LANGSMITH_API_KEY": "test-key"},
        {"LANGCHAIN_API_KEY": "test-key"},
        {"LANGSMITH_TRACING": "false", "LANGSMITH_API_KEY": "test-key"},
        {"LANGSMITH_TRACING": "true", "LANGSMITH_API_KEY": ""},
    ],
    ids=["unset", "no-key", "legacy-no-key", "key-only", "legacy-key-only", "off", "empty-key"],
)
def test_tracing_off_by_default(monkeypatch, env):
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    def touched(*args, **kwargs):
        raise AssertionError("configure_tracing touched langsmith's global state")

    monkeypatch.setattr(langsmith, "Client", touched)
    monkeypatch.setattr(langsmith, "configure", touched)

    assert configure_tracing(load_settings()) is None


@pytest.mark.parametrize("flag", ["LANGSMITH_TRACING", "LANGCHAIN_TRACING_V2"])
@pytest.mark.parametrize("key", ["LANGSMITH_API_KEY", "LANGCHAIN_API_KEY"])
def test_tracing_on_makes_one_default_client(monkeypatch, clients, flag, key):
    monkeypatch.setenv(flag, "true")
    monkeypatch.setenv(key, "test-key")
    monkeypatch.setenv("LANGSMITH_ENDPOINT", NO_NETWORK)

    client = configure_tracing(load_settings())

    [(built, _)] = clients
    assert client is built
    # The default client of every trace: traceable runs and LangGraph's tracer.
    assert get_cached_client() is client
    assert langchain_get_client() is client


def test_langgraph_tracer_uses_the_client_and_project(tracing_env, clients):
    client = configure_tracing(load_settings(env="test"))

    # The tracer that langchain-core adds to each graph invocation when tracing is on.
    manager = CallbackManager.configure()

    [tracer] = [h for h in manager.handlers if isinstance(h, LangChainTracer)]
    assert tracer.client is client
    assert tracer.project_name == "langgraph-postgres-starter-test"


def test_client_takes_the_sampling_rate(tracing_env, clients):
    client = configure_tracing(load_settings(trace_sampling_rate=0.25))

    assert client is not None
    assert client.tracing_sample_rate == 0.25


def test_project_defaults_to_one_per_env(tracing_env, clients):
    configure_tracing(load_settings(env="test"))

    assert get_tracing_context()["project_name"] == "langgraph-postgres-starter-test"


@pytest.mark.parametrize("name", ["LANGSMITH_PROJECT", "LANGCHAIN_PROJECT"])
def test_explicit_project_is_kept(tracing_env, clients, monkeypatch, name):
    monkeypatch.setenv(name, "my-project")

    configure_tracing(load_settings(env="test"))

    # Nothing set in code, so langsmith takes the project from the variable.
    assert get_tracing_context()["project_name"] is None


def test_tracing_errors_are_logged_redacted(tracing_env, clients, capsys):
    configure_logging(load_settings())
    configure_tracing(load_settings())
    [(_, kwargs)] = clients

    kwargs["tracing_error_callback"](RuntimeError("POST /runs failed: password=hunter2"))

    out = capsys.readouterr().out
    [line] = [json.loads(text) for text in out.splitlines() if '"tracing.error"' in text]
    assert line["level"] == "warning"
    assert line["error"] == {
        "type": "RuntimeError",
        "message": "POST /runs failed: password=[REDACTED]",
    }
    assert "hunter2" not in out


class RecordingClient(REAL_CLIENT):
    """A real offline client that records close() calls, and fails them when asked."""

    def __init__(self, fail: bool = False) -> None:
        super().__init__(**offline(api_url=NO_NETWORK, api_key="test-key"))
        self.fail = fail
        self.closed_with: list[float | None] = []

    def close(self, timeout: float | None = None) -> None:
        self.closed_with.append(timeout)
        super().close(timeout=0)
        if self.fail:
            raise RuntimeError("flush failed: password=hunter2")


async def test_close_tracing_flushes_within_the_timeout():
    client = RecordingClient()

    await close_tracing(client)

    assert client.closed_with == [FLUSH_TIMEOUT_S]


async def test_close_tracing_logs_a_failure_and_returns(capsys):
    configure_logging(load_settings())
    client = RecordingClient(fail=True)

    await close_tracing(client)

    out = capsys.readouterr().out
    [line] = [json.loads(text) for text in out.splitlines() if '"tracing.flush_failed"' in text]
    assert line["error"] == {
        "type": "RuntimeError",
        "message": "flush failed: password=[REDACTED]",
    }
    assert "hunter2" not in out


# --- content modes -------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("full", {"log": "password=hunter2 boom"}),
        ("redacted", {"log": "password=[REDACTED] boom"}),
        ("none", {}),
    ],
    ids=["full", "redacted", "none"],
)
def test_content_hooks(mode, expected):
    payload = {"log": "password=hunter2 boom"}
    hide_inputs, hide_outputs = content_hooks(mode)

    assert hide_inputs(payload) == expected
    assert hide_outputs(payload) == expected
    assert payload == {"log": "password=hunter2 boom"}


def test_anonymizer_redacts_secrets_at_any_depth(tracing_env, clients):
    configure_tracing(load_settings())
    [(_, kwargs)] = clients
    payload: object = "password=hunter2"
    # Deeper than langsmith's own create_anonymizer goes by default (10 levels).
    for _ in range(30):
        payload = {"level": [payload]}
    # A secret in a key too, and values that are not strings.
    payload = {
        "user=a@example.com": payload,
        "dsn": "postgresql://lps:hunter2@db/lps",
        "tokens": 1200,
        "cached": None,
    }

    result = kwargs["anonymizer"](payload)

    text = json.dumps(result)
    assert "hunter2" not in text
    assert "a@example.com" not in text
    assert text.count("[REDACTED]") == 3
    assert (result["tokens"], result["cached"]) == (1200, None)


# One run as the client would upload it, with a secret in every field.
RAW_INPUTS = {"evidence": {"logs": [{"line": "db password=hunter2 refused"}]}}
RAW_OUTPUTS = {"summary": "retry with password=hunter2"}
RAW_ERROR = "OperationalError: postgresql://lps:hunter2@db/lps refused"
RAW_METADATA = {"note": "api_key=hunter2"}
RUN = {
    "name": "analyze",
    "run_type": "chain",
    "session_name": "test",
    "inputs": RAW_INPUTS,
    "outputs": RAW_OUTPUTS,
    "error": RAW_ERROR,
    "extra": {"metadata": RAW_METADATA},
}
REDACTED_INPUTS = {"evidence": {"logs": [{"line": "db password=[REDACTED] refused"}]}}
REDACTED_OUTPUTS = {"summary": "retry with password=[REDACTED]"}
REDACTED_ERROR = "OperationalError: postgresql://lps:[REDACTED]@db/lps refused"
REDACTED_METADATA = {"note": "api_key=[REDACTED]"}

# What the client sends in each mode: inputs, outputs, error and metadata.
SENT = {
    "full": (RAW_INPUTS, RAW_OUTPUTS, RAW_ERROR, RAW_METADATA),
    "redacted": (REDACTED_INPUTS, REDACTED_OUTPUTS, REDACTED_ERROR, REDACTED_METADATA),
    "none": ({}, {}, REDACTED_ERROR, REDACTED_METADATA),
}


@pytest.mark.parametrize("mode", ["full", "redacted", "none"])
def test_installed_client_sends_what_the_mode_allows(tracing_env, clients, mode):
    client = configure_tracing(load_settings(trace_content=mode))
    assert client is not None

    # The client's own processing of a run before upload (Client._run_transform in
    # langsmith 0.14.17). It is private: this test pins the precedence of anonymizer and
    # hide_inputs/hide_outputs that configure_tracing relies on.
    sent = client._run_transform(copy.deepcopy(RUN))

    assert (sent["inputs"], sent["outputs"], sent["error"], sent["extra"]["metadata"]) == (
        SENT[mode]
    )


# --- trace_llm -----------------------------------------------------------------------


@dataclass
class FakeUsage:
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int


@dataclass
class FakeResponse:
    text: str
    stop_reason: str
    model_id: str
    usage: FakeUsage


@dataclass
class BareResponse:
    text: str


RESPONSE = FakeResponse(
    text='{"category": "oom"}',
    stop_reason="end_turn",
    model_id="anthropic.claude-opus-5-5",
    usage=FakeUsage(input_tokens=100, output_tokens=20, cache_read_input_tokens=30),
)


async def test_trace_llm_records_usage(offline_client):
    runs = []

    @trace_llm("bedrock.analyze")
    async def analyze(prompt: str) -> FakeResponse:
        runs.append(get_current_run_tree())
        return RESPONSE

    with tracing_context(enabled="local", client=offline_client):
        result = await analyze("why did checkout crash?")

    assert result is RESPONSE
    [run] = runs
    assert run is not None
    assert (run.name, run.run_type) == ("bedrock.analyze", "llm")
    assert run.inputs == {"prompt": "why did checkout crash?"}
    assert run.metadata["model_id"] == "anthropic.claude-opus-5-5"
    assert run.metadata["usage"] == {
        "input_tokens": 100,
        "output_tokens": 20,
        "cache_read_input_tokens": 30,
    }
    # LangSmith's token form: input_tokens counts all input, cache reads included.
    assert run.metadata["usage_metadata"] == {
        "input_tokens": 130,
        "output_tokens": 20,
        "total_tokens": 150,
        "input_token_details": {"cache_read": 30},
    }


def test_trace_llm_wraps_sync_functions_and_tolerates_missing_attributes(offline_client):
    runs = []
    response = BareResponse(text="hi")

    @trace_llm("bedrock.analyze")
    def analyze() -> BareResponse:
        runs.append(get_current_run_tree())
        return response

    with tracing_context(enabled="local", client=offline_client):
        assert analyze() is response

    [run] = runs
    assert run is not None
    assert run.run_type == "llm"
    assert not {"model_id", "usage", "usage_metadata"} & run.metadata.keys()


async def test_trace_llm_without_tracing_returns_the_response():
    @trace_llm("bedrock.analyze")
    async def analyze() -> FakeResponse:
        assert get_current_run_tree() is None
        return RESPONSE

    assert await analyze() is RESPONSE


# --- run metadata --------------------------------------------------------------------


def _run(run_input: dict[str, object]) -> RunRow:
    return RunRow(
        run_id=new_id(),
        agent="incident_triage",
        dedup_key="d" * 64,
        status=RunStatus.RUNNING,
        input=run_input,
        attempt=0,
        lease_owner="pod-a",
        lease_expires_at=None,
        created_at=datetime.now(UTC),
        started_at=None,
        ended_at=None,
        error=None,
        checkpoints_pruned_at=None,
    )


def test_metadata_has_no_secrets():
    settings = load_settings(
        env="test",
        git_sha="abc123",
        cluster_name="eu-prod-1",
        dd_api_key="SECRET",
        dd_app_key="SECRET-APP",
        git_token="SECRET-GIT",  # noqa: S106 - a fake secret that must not leak
        langgraph_aes_key="SECRET-AES-KEY16",
        database_url="postgresql://lps:dsn-SECRET@db:5432/lps",
        migration_database_url="postgresql://owner:dsn-SECRET@db:5432/lps",
    )
    run = _run(
        {
            "namespace": "shop",
            "workload": {"kind": "Deployment", "name": "checkout"},
            "raw": {"token": "SECRET-IN-INPUT"},
        }
    )

    md = run_metadata(run, settings)

    assert "SECRET" not in json.dumps(md)
    assert md == {
        "run_id": str(run.run_id),
        "dedup_key": "d" * 64,
        "agent": "incident_triage",
        "cluster": "eu-prod-1",
        "env": "test",
        "app_version": __version__,
        "git_sha": "abc123",
        "namespace": "shop",
        "workload": "Deployment/checkout",
    }


@pytest.mark.parametrize(
    "run_input",
    [{"key": "a"}, {"namespace": None, "workload": None, "pod": "checkout-7f9c"}],
    ids=["absent", "null"],
)
def test_metadata_without_namespace_or_workload(run_input):
    md = run_metadata(_run(run_input), load_settings())

    assert md.keys() == {"run_id", "dedup_key", "agent", "cluster", "env", "app_version", "git_sha"}
    assert all(isinstance(value, str) for value in md.values())
