from pathlib import Path
from typing import get_args

import pytest
from pydantic import SecretStr, ValidationError

import langgraph_postgres_starter
from langgraph_postgres_starter.settings import (
    MissingSecretsError,
    Settings,
    get_settings,
    load_settings,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = Path(langgraph_postgres_starter.__file__).parent

# The field list from the plan (Task 2), with every default.
EXPECTED_DEFAULTS = {
    "env": "dev",
    "database_url": None,
    "migration_database_url": None,
    "db_pool_min": 1,
    "db_pool_max": 10,
    "db_max_lifetime_s": 1800,
    "db_connect_timeout_s": 10,
    "pgbouncer_mode": False,
    "cluster_name": "local",
    "aws_region": "us-east-1",
    "sqs_queue_url": None,
    "sqs_endpoint_url": None,
    "sqs_wait_seconds": 20,
    "result_queue_url": None,
    "result_sinks": ["log"],
    "worker_concurrency": 4,
    "lease_seconds": 60,
    "heartbeat_seconds": 15,
    "recovery_interval_seconds": 30,
    "drain_timeout_seconds": 45,
    "max_attempts": 3,
    "run_deadline_seconds": 900,
    "health_port": 8080,
    "adapters": "simulated",
    "llm_mode": "recorded",
    "bedrock_model_id": "anthropic.claude-opus-5-5",
    "judge_model_id": None,
    "model_effort": "medium",
    "max_output_tokens": 16000,
    "log_tail_lines": 500,
    "dd_lookback_minutes": 30,
    "dd_max_events": 50,
    "code_budget_chars": 60000,
    "source_cache_dir": Path("/tmp/lps-source-cache"),  # noqa: S108 - the documented default
    "source_cache_max_mb": 2048,
    "source_map_file": None,
    "source_annotation_repo_key": "lps.dev/source-repo",
    "source_annotation_commit_key": "lps.dev/source-commit",
    "git_token": None,
    "dd_api_key": None,
    "dd_app_key": None,
    "dd_site": "datadoghq.com",
    "trace_content": "redacted",
    "trace_sampling_rate": 1.0,
    "git_sha": "unknown",
    "retention_checkpoint_days": 14,
    "retention_result_days": 90,
    "fixtures_dir": PACKAGE_DIR / "agents" / "incident_triage" / "fixtures",
    "studio_fixture": "python-keyerror",
    "extra_agents": [],
    "intake_agent": "incident_triage",
    "fake_call_log_dir": None,
    "fake_datadog_delay_s": 0.0,
}

PROD_SECRETS = {"database_url": "postgresql://db.internal/app", "sqs_queue_url": "https://sqs/q"}


def test_defaults_match_spec():
    s = load_settings()
    assert (
        s.worker_concurrency,
        s.lease_seconds,
        s.heartbeat_seconds,
        s.recovery_interval_seconds,
        s.drain_timeout_seconds,
        s.max_attempts,
        s.run_deadline_seconds,
    ) == (4, 60, 15, 30, 45, 3, 900)
    assert (s.log_tail_lines, s.dd_lookback_minutes, s.dd_max_events, s.code_budget_chars) == (
        500,
        30,
        50,
        60_000,
    )
    assert (s.retention_checkpoint_days, s.retention_result_days, s.health_port) == (14, 90, 8080)
    assert (s.bedrock_model_id, s.model_effort, s.max_output_tokens) == (
        "anthropic.claude-opus-5-5",
        "medium",
        16000,
    )


def test_every_field_and_default_matches_the_plan():
    assert load_settings().model_dump() == EXPECTED_DEFAULTS


def test_env_prefix_and_vendor_names(monkeypatch):
    monkeypatch.setenv("APP_WORKER_CONCURRENCY", "7")
    monkeypatch.setenv("DD_API_KEY", "k")
    monkeypatch.setenv("DD_SITE", "us5.datadoghq.com")
    s = load_settings()
    assert s.worker_concurrency == 7
    assert s.dd_api_key is not None and s.dd_api_key.get_secret_value() == "k"
    assert s.dd_site == "us5.datadoghq.com"


def test_list_fields_parse_json_from_env(monkeypatch):
    monkeypatch.setenv("APP_EXTRA_AGENTS", '["tests.graphs.steps:STEPS_SPEC"]')
    monkeypatch.setenv("APP_RESULT_SINKS", '["log","sqs"]')
    s = load_settings()
    assert s.extra_agents == ["tests.graphs.steps:STEPS_SPEC"]
    assert s.result_sinks == ["log", "sqs"]


def test_empty_env_value_keeps_the_default(monkeypatch):
    monkeypatch.setenv("APP_DATABASE_URL", "")
    monkeypatch.setenv("APP_CLUSTER_NAME", "")
    s = load_settings()
    assert s.database_url is None and s.cluster_name == "local"


def test_overrides_use_field_names_including_aliased_fields(monkeypatch):
    monkeypatch.setenv("APP_WORKER_CONCURRENCY", "7")
    s = load_settings(worker_concurrency=2, dd_app_key="a", dd_site="datadoghq.eu")
    assert s.worker_concurrency == 2
    assert s.dd_app_key is not None and s.dd_app_key.get_secret_value() == "a"
    assert s.dd_site == "datadoghq.eu"


def test_unknown_override_is_rejected():
    with pytest.raises(ValidationError, match="worker_concurency"):
        load_settings(worker_concurency=2)


def test_get_settings_is_cached():
    assert get_settings() is get_settings()


def test_prod_requires_secrets():
    with pytest.raises(MissingSecretsError) as e:
        load_settings(env="prod", adapters="real").require_runtime_secrets()
    assert {"database_url", "sqs_queue_url", "dd_api_key", "dd_app_key"} <= set(e.value.missing)


def test_prod_with_simulated_adapters_needs_no_datadog_keys(monkeypatch):
    monkeypatch.setenv("APP_DATABASE_URL", "")  # empty counts as missing
    with pytest.raises(MissingSecretsError) as e:
        load_settings(env="prod").require_runtime_secrets()
    assert e.value.missing == ["database_url", "sqs_queue_url"]
    load_settings(env="prod", **PROD_SECRETS).require_runtime_secrets()


def test_prod_with_every_secret_passes():
    load_settings(
        env="prod", adapters="real", dd_api_key="k", dd_app_key="a", **PROD_SECRETS
    ).require_runtime_secrets()


def test_prod_rejects_full_trace_content():
    s = load_settings(env="prod", trace_content="full", **PROD_SECRETS)
    with pytest.raises((MissingSecretsError, ValueError), match="trace_content"):
        s.require_runtime_secrets()


def test_secret_check_applies_only_in_prod():
    for env in ("dev", "test"):
        load_settings(env=env, adapters="real", trace_content="full").require_runtime_secrets()


def _env_name(field_name: str) -> str:
    alias = Settings.model_fields[field_name].validation_alias
    return alias if isinstance(alias, str) else f"APP_{field_name.upper()}"


def _env_example() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in (REPO_ROOT / ".env.example").read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            name, _, value = line.partition("=")
            values[name] = value
    return values


def test_env_example_documents_every_field_with_its_default(monkeypatch):
    documented = _env_example()
    assert sorted(documented) == sorted(_env_name(name) for name in Settings.model_fields)
    defaults = load_settings().model_dump()
    for name, value in documented.items():
        monkeypatch.setenv(name, value)
    assert load_settings().model_dump() == defaults


def test_env_example_leaves_secrets_empty():
    documented = _env_example()
    secrets = [
        _env_name(name)
        for name, field in Settings.model_fields.items()
        if SecretStr in get_args(field.annotation)
    ]
    assert {"APP_GIT_TOKEN", "DD_API_KEY", "DD_APP_KEY"} <= set(secrets)
    assert all(documented[name] == "" for name in secrets)
