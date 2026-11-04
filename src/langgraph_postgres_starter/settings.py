"""Application settings, read from environment variables.

Every variable has the ``APP_`` prefix, except the Datadog names, which keep their
vendor form: ``DD_API_KEY``, ``DD_APP_KEY`` and ``DD_SITE``. List values are JSON,
for example ``APP_EXTRA_AGENTS=["my_pkg.graph:SPEC"]``. An empty value means "not
set", so the field keeps its default. ``.env.example`` documents every variable.

Settings reads the process environment only, never a ``.env`` file. ``langgraph
dev`` and Docker Compose load ``.env`` into the environment themselves.
"""

import functools
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_PACKAGE_DIR = Path(__file__).parent


class MissingSecretsError(RuntimeError):
    """A required secret is not set in ``prod``. ``missing`` names the settings fields."""

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        super().__init__(f"settings required when env='prod' are not set: {', '.join(missing)}")


class Settings(BaseSettings):
    """All runtime configuration. See ``.env.example`` for each variable and its default."""

    model_config = SettingsConfigDict(
        env_prefix="APP_",
        # Empty variables (as in a copied .env.example) keep the field's default.
        env_ignore_empty=True,
        # Lets load_settings() take every override by field name, aliased fields included.
        validate_by_name=True,
    )

    env: Literal["dev", "test", "prod"] = "dev"
    git_sha: str = "unknown"

    # Postgres
    database_url: str | None = None
    migration_database_url: str | None = None
    db_pool_min: int = 1
    db_pool_max: int = 10
    db_max_lifetime_s: int = 1800
    db_connect_timeout_s: int = 10
    pgbouncer_mode: bool = False

    # Queues (SQS; ElasticMQ locally)
    aws_region: str = "us-east-1"
    sqs_queue_url: str | None = None
    sqs_endpoint_url: str | None = None
    sqs_wait_seconds: int = 20
    result_queue_url: str | None = None
    result_sinks: list[Literal["log", "sqs"]] = ["log"]

    # Worker and retention
    worker_concurrency: int = 4
    lease_seconds: int = 60
    heartbeat_seconds: int = 15
    recovery_interval_seconds: int = 30
    drain_timeout_seconds: int = 45
    max_attempts: int = 3
    run_deadline_seconds: int = 900
    health_port: int = 8080
    retention_checkpoint_days: int = 14
    retention_result_days: int = 90

    # Agents
    intake_agent: str = "incident_triage"
    extra_agents: list[str] = []

    # Adapters and model
    adapters: Literal["simulated", "real"] = "simulated"
    llm_mode: Literal["recorded", "bedrock"] = "recorded"
    bedrock_model_id: str = "anthropic.claude-opus-5-5"
    judge_model_id: str | None = None
    model_effort: Literal["low", "medium", "high", "xhigh", "max"] = "medium"
    max_output_tokens: int = 16000

    # Kubernetes evidence
    cluster_name: str = "local"
    log_tail_lines: int = 500

    # Datadog evidence (vendor variable names, no APP_ prefix)
    dd_api_key: SecretStr | None = Field(default=None, validation_alias="DD_API_KEY")
    dd_app_key: SecretStr | None = Field(default=None, validation_alias="DD_APP_KEY")
    dd_site: str = Field(default="datadoghq.com", validation_alias="DD_SITE")
    dd_lookback_minutes: int = 30
    dd_max_events: int = 50

    # Code evidence
    git_token: SecretStr | None = None
    code_budget_chars: int = 60000
    source_cache_dir: Path = Path("/tmp/lps-source-cache")  # noqa: S108 - the documented default
    source_cache_max_mb: int = 2048
    source_map_file: Path | None = None
    source_annotation_repo_key: str = "lps.dev/source-repo"
    source_annotation_commit_key: str = "lps.dev/source-commit"

    # Tracing (LangSmith)
    trace_content: Literal["full", "redacted", "none"] = "redacted"
    trace_sampling_rate: float = 1.0

    # Local runs, Studio and tests (simulated adapters)
    fixtures_dir: Path = _PACKAGE_DIR / "agents" / "incident_triage" / "fixtures"
    studio_fixture: str = "python-keyerror"
    fake_call_log_dir: Path | None = None
    fake_datadog_delay_s: float = 0.0

    def require_runtime_secrets(self) -> None:
        """Stop a start that would trace full content, or a ``prod`` start that lacks a secret.

        Raises ValueError when ``trace_content`` is ``full`` and ``env`` is not ``dev``.
        In ``prod`` only, raises MissingSecretsError that names every missing setting.
        """
        if self.trace_content == "full" and self.env != "dev":
            raise ValueError(
                "trace_content='full' (APP_TRACE_CONTENT) is allowed only when env='dev', "
                f"not env={self.env!r}; use 'redacted' or 'none'"
            )
        if self.env != "prod":
            return
        required = ["database_url", "sqs_queue_url"]
        if self.adapters == "real":
            required += ["dd_api_key", "dd_app_key"]
        missing = [name for name in required if getattr(self, name) is None]
        if missing:
            raise MissingSecretsError(missing)


def load_settings(**overrides: Any) -> Settings:
    """Build settings from the environment. ``overrides`` take field names and win."""
    return Settings(**overrides)


@functools.cache
def get_settings() -> Settings:
    """Return the process-wide settings, built once on first call."""
    return load_settings()
