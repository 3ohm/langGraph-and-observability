"""The example's own trace metadata and the settings it needs in prod (spec §12.1; P14).

The core trace keys are tested in ``tests/unit/test_tracing.py``; the executor merges
these keys under them.
"""

import json

import pytest

from langgraph_and_observability.agents.incident_triage.spec import required_settings, spec
from langgraph_and_observability.agents.incident_triage.tracing import trace_metadata
from langgraph_and_observability.settings import load_settings


def test_spec_names_the_settings_check_and_the_trace_metadata():
    assert spec.required_settings is required_settings
    assert spec.trace_metadata is trace_metadata


def test_trace_metadata_has_no_secrets():
    settings = load_settings(
        env="test",
        cluster_name="eu-prod-1",
        dd_api_key="SECRET",
        dd_app_key="SECRET-APP",
        git_token="SECRET-GIT",  # noqa: S106 - a fake secret that must not leak
        langgraph_aes_key="SECRET-AES-KEY16",
        database_url="postgresql://lps:dsn-SECRET@db:5432/lps",
    )
    run_input = {
        "namespace": "shop",
        "workload": {"kind": "Deployment", "name": "checkout"},
        "raw": {"token": "SECRET-IN-INPUT"},
    }

    md = trace_metadata(run_input, settings)

    assert "SECRET" not in json.dumps(md)
    assert md == {"cluster": "eu-prod-1", "namespace": "shop", "workload": "Deployment/checkout"}


@pytest.mark.parametrize(
    "run_input",
    [{"key": "a"}, {"namespace": None, "workload": None, "pod": "checkout-7f9c"}],
    ids=["absent", "null"],
)
def test_trace_metadata_without_namespace_or_workload(run_input):
    md = trace_metadata(run_input, load_settings())

    assert md == {"cluster": "local"}


@pytest.mark.parametrize(
    ("workload", "expected"),
    [
        ({"kind": "StatefulSet", "name": "db"}, {"workload": "StatefulSet/db"}),
        ("Deployment/checkout", {"workload": "Deployment/checkout"}),
        ({"kind": "Deployment"}, {}),
        ({"kind": "Deployment", "name": 7}, {}),
        (["Deployment", "checkout"], {}),
    ],
    ids=["kind-and-name", "string", "no-name", "name-not-a-string", "list"],
)
def test_trace_metadata_workload(workload, expected):
    md = trace_metadata({"workload": workload}, load_settings())

    assert md == {"cluster": "local", **expected}


def test_required_settings_are_the_datadog_keys_with_real_adapters():
    assert list(required_settings(load_settings(adapters="real"))) == ["dd_api_key", "dd_app_key"]
    assert list(required_settings(load_settings(adapters="simulated"))) == []
