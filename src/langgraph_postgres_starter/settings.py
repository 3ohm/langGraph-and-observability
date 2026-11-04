"""Application settings, read from environment variables.

Every variable has the ``APP_`` prefix, except the vendor names, which keep their
vendor form: ``DD_API_KEY``, ``DD_APP_KEY``, ``DD_SITE`` and ``LANGGRAPH_AES_KEY``. List
values are JSON, for example ``APP_EXTRA_AGENTS=["my_pkg.graph:SPEC"]``. An empty value
means "not set", so the field keeps its default. ``.env.example`` documents every
variable. Numeric settings have bounds; a value outside them, or a heartbeat that is
not shorter than the lease, fails validation, so the worker does not start.

Settings reads the process environment only, never a ``.env`` file. ``langgraph
dev`` and Docker Compose load ``.env`` into the environment themselves.
"""

import functools
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import Field, SecretStr, model_validator
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
    # At least one connection; the worker holds one for a prune pass while runs use others.
    db_pool_min: int = Field(default=1, ge=1)
    db_pool_max: int = Field(default=10, ge=2)
    db_max_lifetime_s: int = 1800
    db_connect_timeout_s: int = 10
    pgbouncer_mode: bool = False
    # The checkpoint encryption key (vendor name, no APP_ prefix): 16, 24 or 32 bytes.
    # When set, checkpoints are encrypted at rest; that needs the `encryption` extra.
    langgraph_aes_key: SecretStr | None = Field(default=None, validation_alias="LANGGRAPH_AES_KEY")

    # Queues (SQS; ElasticMQ locally)
    aws_region: str = "us-east-1"
    sqs_queue_url: str | None = None
    sqs_endpoint_url: str | None = None
    # SQS long polling waits 0 to 20 seconds.
    sqs_wait_seconds: int = Field(default=20, ge=0, le=20)
    result_queue_url: str | None = None
    result_sinks: list[Literal["log", "sqs"]] = ["log"]

    # Worker and retention
    # Counts and times of 1 or more: a 0 would stop the worker without an error.
    worker_concurrency: int = Field(default=4, ge=1)
    lease_seconds: int = Field(default=60, ge=1)
    # Below lease_seconds (checked below), so a live run's lease never expires.
    heartbeat_seconds: int = Field(default=15, ge=1)
    recovery_interval_seconds: int = Field(default=30, ge=1)
    drain_timeout_seconds: int = Field(default=45, ge=1)
    max_attempts: int = Field(default=3, ge=1)
    run_deadline_seconds: int = Field(default=900, ge=1)
    health_port: int = Field(default=8080, ge=1, le=65535)
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
    log_tail_lines: int = Field(default=500, ge=1)

    # Datadog evidence (vendor variable names, no APP_ prefix)
    dd_api_key: SecretStr | None = Field(default=None, validation_alias="DD_API_KEY")
    dd_app_key: SecretStr | None = Field(default=None, validation_alias="DD_APP_KEY")
    dd_site: str = Field(default="datadoghq.com", validation_alias="DD_SITE")
    dd_lookback_minutes: int = Field(default=30, ge=0)
    dd_max_events: int = Field(default=50, ge=1, le=1000)

    # Code evidence
    git_token: SecretStr | None = None
    # The only hosts that source checkouts fetch from, and so the only hosts that
    # receive git_token (ruling R59): repository URLs come from cluster metadata.
    git_allowed_hosts: list[str] = ["github.com"]
    code_budget_chars: int = Field(default=60000, ge=1)
    source_cache_dir: Path = Path("/tmp/lps-source-cache")  # noqa: S108 - the documented default
    source_cache_max_mb: int = 2048
    source_map_file: Path | None = None
    source_annotation_repo_key: str = "lps.dev/source-repo"
    source_annotation_commit_key: str = "lps.dev/source-commit"
    # The folder of the tree-sitter grammars, which `make grammars` fetches (ruling R51);
    # None: the library's user cache. The agent never downloads a grammar.
    grammar_dir: Path | None = None

    # Tracing (LangSmith)
    trace_content: Literal["full", "redacted", "none"] = "redacted"
    trace_sampling_rate: float = Field(default=1.0, ge=0, le=1)

    # Local runs, Studio and tests (simulated adapters)
    fixtures_dir: Path = _PACKAGE_DIR / "agents" / "incident_triage" / "fixtures"
    studio_fixture: str = "python-keyerror"
    fake_call_log_dir: Path | None = None
    fake_datadog_delay_s: float = 0.0

    @model_validator(mode="after")
    def _check_related_settings(self) -> Self:
        """Settings that bound each other: the pool's size, and heartbeats within a lease."""
        if self.db_pool_max < self.db_pool_min:
            raise ValueError(
                f"db_pool_max ({self.db_pool_max}) must not be below db_pool_min "
                f"({self.db_pool_min})"
            )
        if self.heartbeat_seconds >= self.lease_seconds:
            raise ValueError(
                f"heartbeat_seconds ({self.heartbeat_seconds}) must be below lease_seconds "
                f"({self.lease_seconds}), or a live run's lease expires between heartbeats"
            )
        return self

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
