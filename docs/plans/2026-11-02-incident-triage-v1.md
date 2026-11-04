# langgraph-postgres-starter v1 (Incident Triage) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Phases 0–1 of the spec. That means the repo foundation, a durable SQS worker, and the Incident Triage agent with both simulated and real adapters.

**Architecture:**
- Each agent is a LangGraph `StateGraph`, checkpointed to Postgres after every step (`durability="sync"`).
- A queue worker records each SQS message as a `runs` row, then executes it under a lease. The worker sends heartbeats, runs a periodic recovery check, and drains on SIGTERM.
- The triage graph collects Kubernetes evidence, Datadog evidence, and code evidence when needed, all through adapter interfaces that have fakes.
- The graph then makes one structured-output call to Claude on Bedrock via boto3, and publishes a typed result idempotently.

**Tech Stack:**
- Python 3.14 and uv
- LangGraph 1.2 and langgraph-checkpoint-postgres 3.2
- psycopg 3.3 with psycopg-pool
- pydantic 2 and pydantic-settings
- kubernetes-asyncio, datadog-api-client and boto3
- tree-sitter with tree-sitter-language-pack, and ripgrep
- FastAPI (health only) and structlog
- LangSmith
- pytest and testcontainers
- Postgres 18, ElasticMQ and Docker

**Spec:** `docs/design/2026-11-01-langgraph-postgres-starter-design.md` (v2). Section signs (§) below refer to it. Read §6–§13 before Task 1.

**Execution set-up (owner's decisions):**
- Implementer subagents use the `implementer` agent (Claude Opus 5.5).
- Reviewer subagents use the `reviewer` agent (Claude Fable 5.1).
- Both agents are defined in `.claude/agents/`.
- A Docker runtime must be running from Task 4 onward.
- Every message to the owner is written in ASD-STE100 Simplified Technical English.

## Global Constraints

- **Python:** `requires-python = ">=3.14"`, and `.python-version` is `3.14`. Use standard (GIL) CPython only. CI also runs a Python 3.15 job with `continue-on-error: true`.
- **Dependency pins:** `langgraph>=1.2.19,<2`, `langgraph-checkpoint>=4.3.1,<5`, `langgraph-checkpoint-postgres>=3.2.0,<4`, `langchain-core>=1.7.3,<2`, `langsmith>=0.14.16,<1`.
- **Package and settings names:** the package is `langgraph_postgres_starter` under `src/`. Settings use the `APP_` prefix. Vendor variables keep their own names: `DD_API_KEY`, `DD_APP_KEY`, `DD_SITE`, `LANGSMITH_*`, `LANGGRAPH_*`, `AWS_*`.
- **Images:** `pgvector/pgvector:0.8.4-pg18`, `softwaremill/elasticmq-native:1.7.1`, and the runtime `python:3.14-slim` with `ripgrep`, `git` and `ca-certificates`.
- **Async nodes and adapters:** every graph node is `async`. Every external system is reached only through an adapter Protocol that has a fake.
- **Where vendor SDKs may be imported:** `kubernetes_asyncio`, `datadog_api_client`, `boto3`/`botocore` and `langsmith` appear only under `agents/*/adapters/`, `worker/intake.py`, `observability/` and `policies.py` (the last for exception types only).
- **Graph runs:** `thread_id == str(run_id)`, where `run_id` is a `uuid.uuid7()`. `durability="sync"`. `configurable` holds only `thread_id`.
- **Bedrock:**
  - default model ID `anthropic.claude-opus-5-5`;
  - never send `temperature`, `top_p` or `top_k`;
  - never force `tool_choice`, and use no assistant prefill;
  - effort defaults to `medium`, and `max_tokens` to 16000;
  - handle `stop_reason == "refusal"` and `"max_tokens"`.
- **Retries:** only one layer, inside the client library (botocore `mode="standard"`, `max_attempts=3`; Datadog `enable_retry=True`). The graph's `RetryPolicy` retries only what `policies.is_transient` accepts (Task 6), and never a 4xx except Kubernetes 429.
- **Secrets:** no secrets in graph state, `configurable`, traces or prompts. All evidence text passes `redaction.redact()` before it enters state.
- **Defaults:**

  | Setting | Default |
  |---|---|
  | worker concurrency | 4 |
  | lease | 60 s |
  | heartbeat | 15 s |
  | recovery interval | 30 s |
  | drain timeout | 45 s |
  | max attempts | 3 |
  | run deadline | 900 s |
  | log tail | 500 lines |
  | Datadog window | −30 min / +5 min |
  | Datadog max events | 50 |
  | code budget | 60,000 chars |
  | checkpoint retention | 14 days |
  | result retention | 90 days |
  | health port | 8080 |

- **Tests:**
  - Warnings are errors. The one named filter is `ignore:'asyncio.iscoroutinefunction' is deprecated:DeprecationWarning:langsmith`.
  - Markers: `integration` (needs Docker) and `e2e` (starts worker subprocesses).
  - Unit tests run with no Docker, no network and no credentials.
- **GitHub Actions:** pinned to full 40-hex commit SHAs, with `permissions: contents: read` unless a job needs more.
- **Tree-sitter grammars:** `javascript`, `typescript`, `tsx`, `java`, `kotlin`, `scala`, `python`, `go`, `php`, `ruby`, `c`, `cpp`.
- **Commits:** commit after every task with a Conventional Commits subject.

## Review Focus

1. **The pod in the message no longer exists.** It may have been replaced after a crash, or a finished Job pod may have been deleted. Expected: resolve to the owner workload's current pod. If nothing exists, continue with a `note:target:not_found` item. Tests (Task 23): `test_resolve_pod_gone_falls_back_to_workload`, `test_resolve_nothing_found_continues`.
2. **Multi-container pods with sidecars** such as `istio-proxy` or `datadog-agent`. Expected: use the container named in the message. Otherwise use the container whose last termination was non-zero, then the one with the most restarts. Never pick a sidecar by default. Test (Task 23): `test_pick_container_ignores_healthy_sidecar`.
3. **No logs from the previous container.** This happens on a first crash, after log rotation, or when the API returns 400. Expected: the step succeeds and records `note:logs:previous_unavailable`. Test (Task 23): `test_previous_logs_unavailable_is_a_note`.
4. **No usable source code.** Stack frames may not map to files at the deployed commit, or there may be no source location at all. Expected: `collect_code` returns a note and the run continues to `analyze`. Tests (Task 30): `test_collect_code_without_source_continues`, `test_unmapped_frames_are_recorded`.
5. **Truncated or throttled Bedrock replies.** Expected: `stop_reason == "max_tokens"` counts as a validation failure and gets the one repair attempt. Throttling is retried by botocore, not by the graph. Tests (Task 31): `test_max_tokens_stop_triggers_repair`, `test_throttling_left_to_botocore`.

---

## File Structure

| Path (under `src/langgraph_postgres_starter/` unless rooted) | Responsibility | Task |
|---|---|---|
| `__init__.py`, `__main__.py` | version; CLI entry | 1, 15 |
| `settings.py`, `logging.py`, `ids.py` | config, JSON logs, uuid7 | 2 |
| `redaction.py` | secret/PII redaction | 5 |
| `policies.py` | retry predicate, retry and timeout policies | 6 |
| `agent_spec.py` | `AgentSpec`, `RunInfo` (no imports of agents or worker) | 11 |
| `persistence/pool.py`, `persistence/resources.py` | pool factory; process-level pool handle | 7 |
| `persistence/migrate.py`, `persistence/migrations/` | migration runner; `action_ledger` | 8 |
| `persistence/serde.py`, `persistence/checkpointer.py` | strict serde; pool-aware saver | 9 |
| `persistence/ledger.py` | idempotent actions | 10 |
| `worker/registry.py` | built-in agent list (one line per agent) | 11, 33 |
| `worker/runs.py`, `worker/migrations/` | `runs` table and repository | 12, 17 |
| `worker/executor.py` | execute one run under lease and deadline | 13, 19 |
| `worker/intake.py` | SQS → runs | 14 |
| `worker/health.py`, `worker/cli.py` | `/ok`, `/ready`; CLI | 15 |
| `worker/main.py` | loops: intake, recovery, heartbeat, pruner; drain | 16, 17 |
| `worker/retention.py` | pruner | 17 |
| `observability/langsmith.py`, `observability/metadata.py` | tracing; run metadata | 19 |
| `agents/incident_triage/schemas.py`, `state.py`, `dedup.py` | types, state, dedup key | 20 |
| `agents/incident_triage/adapters/**`, `context.py`, `fixtures/loader.py` | interfaces, fakes, real adapters, context | 21, 23, 24, 28, 31, 32 |
| `agents/incident_triage/fixtures/{apps,incidents,recorded}/` | 12 sample apps, 16 incidents, recorded replies | 22, 31, 33 |
| `agents/incident_triage/evidence.py`, `nodes/resolve.py`, `nodes/k8s.py` | evidence helpers; first two steps | 23 |
| `agents/incident_triage/nodes/datadog.py` | Datadog step | 24 |
| `agents/incident_triage/codeintel/stacktrace/*` | per-family parsers | 25, 26 |
| `agents/incident_triage/assess.py` | rules and routing | 27 |
| `agents/incident_triage/codeintel/paths.py`, `adapters/source/*` | locate, check out, map paths | 28 |
| `agents/incident_triage/codeintel/languages.py`, `extract.py` | tree-sitter | 29 |
| `agents/incident_triage/codeintel/search.py`, `bundle.py`, `nodes/code.py` | ripgrep, bundle, code step | 30 |
| `agents/incident_triage/prompts.py`, `adapters/llm/bedrock.py`, `nodes/analyze.py` | analysis | 31 |
| `agents/incident_triage/nodes/publish.py`, `adapters/sinks/*`, `migrations/` | results | 32 |
| `agents/incident_triage/graph.py`, `spec.py`, `langgraph.json` | assembly | 33 |
| `agents/incident_triage/evals/` | evals | 35 |
| `tests/graphs/steps.py` | test-only 3-step agent for core practices | 11 |
| `deploy/elasticmq/`, `deploy/sql/roles.sql`, `deploy/k8s/`, `deploy/iam/` | infra examples | 4, 8, 36 |
| `scripts/` | `send_incident.py`, `sync_dataset.py`, `rename.py`, `remove_example.py` | 34, 35, 37 |
| `docs/practices/P1–P23.md`, `docs/guides/*` | notes and guides | 37, 38 |

---

### Task 1: Project skeleton, tooling and governance files

**Files:**
- Create:
  - `pyproject.toml`, `.python-version`, `.gitignore`, `.editorconfig`, `.pre-commit-config.yaml`, `Makefile`
  - `src/langgraph_postgres_starter/__init__.py`, `src/langgraph_postgres_starter/__main__.py`
  - `tests/conftest.py`, `tests/unit/test_package.py`
  - `LICENSE` (MIT), `README.md` (stub), `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md` (Contributor Covenant 2.1), `SECURITY.md`, `CHANGELOG.md` (Keep a Changelog, "Unreleased")
  - `AGENTS.md` (rules from spec §13.8), `CLAUDE.md` (single line `@AGENTS.md`)
  - `docs/adr/0001-template-not-framework.md`
  - `.github/ISSUE_TEMPLATE/{bug.yml,feature.yml,config.yml}`, `.github/PULL_REQUEST_TEMPLATE.md`
- Keep (already present): `docs/design/`, `docs/plans/`, `.claude/agents/`

**Interfaces:**
- Produces:
  - `langgraph_postgres_starter.__version__: str`, read from package metadata. The version in `pyproject.toml` is `0.1.0.dev0`.
  - `python -m langgraph_postgres_starter --version`.
  - Make targets `setup`, `lint`, `fmt`, `typecheck`, `test`.

- [ ] **Step 1:** Run `git init -b main` in the repo root. Run `git status` and confirm that `docs/` and `.claude/` are untracked.
- [ ] **Step 2: Write the failing test** `tests/unit/test_package.py`

```python
import subprocess, sys
from importlib.metadata import version
import langgraph_postgres_starter as pkg

def test_version_matches_metadata():
    assert pkg.__version__ == version("langgraph-postgres-starter") == "0.1.0.dev0"

def test_cli_prints_version():
    out = subprocess.run([sys.executable, "-m", "langgraph_postgres_starter", "--version"],
                         capture_output=True, text=True, check=True)
    assert out.stdout.strip() == pkg.__version__
```

- [ ] **Step 3:** Run `uv run pytest tests/unit/test_package.py -q`. Expected: FAIL (`ModuleNotFoundError`).
- [ ] **Step 4: Write `pyproject.toml`.** Use the hatchling build backend with `src/` layout.
  - Runtime dependencies use the Global Constraints floors: langgraph, langgraph-checkpoint-postgres, psycopg[binary,pool], pydantic, pydantic-settings, structlog, fastapi, uvicorn, kubernetes-asyncio, datadog-api-client, boto3, tree-sitter, tree-sitter-language-pack, langsmith.
  - Dependency groups:
    - `dev`: ruff, mypy, pre-commit, `langgraph-cli[inmem]`
    - `test`: pytest, pytest-asyncio, `testcontainers[postgres]`, httpx, pyyaml
    - `evals`: openevals
  - Optional extra: `encryption = ["pycryptodome"]`.
  - ruff: line length 100, target `py314`, rules `E,F,I,B,UP,ASYNC,S`, with `S101` ignored in tests.
  - mypy: `strict = true`, `python_version = "3.14"`, the pydantic plugin.
  - pytest:
    - `asyncio_mode = "auto"`
    - `testpaths = ["tests", "src/langgraph_postgres_starter/agents"]`
    - the markers `integration` and `e2e`
    - `filterwarnings = ["error", "<the filter from Global Constraints>"]`
- [ ] **Step 5: Write `__init__.py` and `__main__.py`.** `__init__.py` reads `__version__` from `importlib.metadata.version`. `__main__.py` uses argparse with `--version`; Task 15 adds subcommands.
- [ ] **Step 6:** Run `uv sync --all-groups && uv run pytest tests/unit/test_package.py -q`. Expected: `2 passed`.
- [ ] **Step 7: Write the governance files, `AGENTS.md`, `.pre-commit-config.yaml` and `Makefile`.**
  - Pre-commit hooks: ruff, ruff-format, a local `uv run mypy src tests`, gitleaks, check-yaml, end-of-file-fixer, trailing-whitespace, and `uv lock --check`.
  - `make test` runs `uv run pytest -m "not integration and not e2e"`.
- [ ] **Step 8:** Run `make lint typecheck test`. Expected: all pass.
- [ ] **Step 9: Commit.** `git add -A && git commit -m "chore: project skeleton, tooling and governance files"`

### Task 2: Settings, logging and IDs

**Files:**
- Create: `src/langgraph_postgres_starter/{settings.py,logging.py,ids.py}`, `.env.example`
- Test: `tests/unit/{test_settings.py,test_ids.py,test_logging.py}`

**Interfaces:**
- Produces:
  - `class Settings(BaseSettings)`, with `env_prefix="APP_"`. Fields with their defaults are listed below.
  - `def load_settings(**overrides: Any) -> Settings`.
  - `def get_settings() -> Settings` (cached).
  - `Settings.require_runtime_secrets() -> None`, which raises `MissingSecretsError(missing: list[str])`.
  - `ids.new_id() -> uuid.UUID`, which returns `uuid.uuid7()`.
  - `logging.configure_logging(settings: Settings) -> None`.
  - `logging.get_logger(name: str) -> structlog.stdlib.BoundLogger`. It writes JSON to stdout.

```text
env: Literal["dev","test","prod"]="dev"; database_url: str|None; migration_database_url: str|None
db_pool_min=1; db_pool_max=10; db_max_lifetime_s=1800; db_connect_timeout_s=10; pgbouncer_mode=False
cluster_name="local"; aws_region="us-east-1"; sqs_queue_url: str|None; sqs_endpoint_url: str|None
sqs_wait_seconds=20; result_queue_url: str|None; result_sinks: list[Literal["log","sqs"]]=["log"]
worker_concurrency=4; lease_seconds=60; heartbeat_seconds=15; recovery_interval_seconds=30
drain_timeout_seconds=45; max_attempts=3; run_deadline_seconds=900; health_port=8080
adapters: Literal["simulated","real"]="simulated"; llm_mode: Literal["recorded","bedrock"]="recorded"
bedrock_model_id="anthropic.claude-opus-5-5"; judge_model_id: str|None=None
model_effort: Literal["low","medium","high","xhigh","max"]="medium"; max_output_tokens=16000
log_tail_lines=500; dd_lookback_minutes=30; dd_max_events=50; code_budget_chars=60000
source_cache_dir=Path("/tmp/lps-source-cache"); source_cache_max_mb=2048; source_map_file: Path|None
source_annotation_repo_key="lps.dev/source-repo"; source_annotation_commit_key="lps.dev/source-commit"
git_token: SecretStr|None; dd_api_key: SecretStr|None (alias DD_API_KEY); dd_app_key: SecretStr|None (alias DD_APP_KEY)
dd_site="datadoghq.com" (alias DD_SITE); trace_content: Literal["full","redacted","none"]="redacted"
trace_sampling_rate=1.0; git_sha="unknown"; retention_checkpoint_days=14; retention_result_days=90
fixtures_dir=<package>/agents/incident_triage/fixtures; studio_fixture="python-keyerror"
extra_agents: list[str]=[]; intake_agent="incident_triage"; fake_call_log_dir: Path|None=None; fake_datadog_delay_s=0.0
```

`require_runtime_secrets()` applies only when `env == "prod"`.
- It requires `database_url` and `sqs_queue_url`.
- With `adapters == "real"`, it also requires `dd_api_key` and `dd_app_key`.
- It rejects `trace_content == "full"`.

- [ ] **Step 1: Write the failing tests**

```python
def test_defaults_match_spec():
    s = load_settings()
    assert (s.worker_concurrency, s.lease_seconds, s.heartbeat_seconds, s.recovery_interval_seconds,
            s.drain_timeout_seconds, s.max_attempts, s.run_deadline_seconds) == (4, 60, 15, 30, 45, 3, 900)
    assert (s.log_tail_lines, s.dd_lookback_minutes, s.dd_max_events, s.code_budget_chars) == (500, 30, 50, 60_000)
    assert (s.retention_checkpoint_days, s.retention_result_days, s.health_port) == (14, 90, 8080)
    assert (s.bedrock_model_id, s.model_effort, s.max_output_tokens) == ("anthropic.claude-opus-5-5", "medium", 16000)

def test_env_prefix_and_vendor_names(monkeypatch):
    monkeypatch.setenv("APP_WORKER_CONCURRENCY", "7"); monkeypatch.setenv("DD_API_KEY", "k")
    s = load_settings(); assert s.worker_concurrency == 7 and s.dd_api_key.get_secret_value() == "k"

def test_prod_requires_secrets():
    with pytest.raises(MissingSecretsError) as e:
        load_settings(env="prod", adapters="real").require_runtime_secrets()
    assert {"database_url", "sqs_queue_url", "dd_api_key", "dd_app_key"} <= set(e.value.missing)

def test_prod_rejects_full_trace_content(): ...  # raises MissingSecretsError or ValueError naming trace_content

def test_uuid7_time_ordered():
    ids = [new_id() for _ in range(200)]; assert all(i.version == 7 for i in ids) and ids == sorted(ids)

def test_logs_are_json_with_bound_fields(capsys):
    configure_logging(load_settings(env="test")); get_logger("t").bind(run_id="r1").info("hello")
    line = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert line["event"] == "hello" and line["run_id"] == "r1" and "timestamp" in line
```

- [ ] **Step 2:** Run `uv run pytest tests/unit/test_settings.py tests/unit/test_ids.py tests/unit/test_logging.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement the three modules.** Also write `.env.example`, which documents every field above with its default. Secrets are listed there with empty values.
- [ ] **Step 4:** Run the same command. Expected: PASS.
- [ ] **Step 5: Commit.** `feat: settings, JSON logging and uuid7 ids`

### Task 3: CI workflow and Dependabot

**Files:**
- Create: `.github/workflows/ci.yml`, `.github/dependabot.yml`
- Test: `tests/unit/test_ci_config.py`

**Interfaces:**
- Produces CI jobs `lint`, `unit` and `canary-3-15`. Later tasks add `docker` (Task 4), `integration` (Task 7), `e2e` (Task 18) and `removal` (Task 37).

- [ ] **Step 1: Write the failing tests**

```python
WF = yaml.safe_load(Path(".github/workflows/ci.yml").read_text())
def test_actions_pinned_to_sha():
    uses = [s["uses"] for j in WF["jobs"].values() for s in j["steps"] if "uses" in s]
    assert uses and all(re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", u) for u in uses)
def test_minimal_permissions(): assert WF["permissions"] == {"contents": "read"}
def test_canary_does_not_block():
    j = WF["jobs"]["canary-3-15"]; assert j["continue-on-error"] is True and "3.15" in json.dumps(j)
def test_dependabot_weekly_grouped_with_cooldown():
    d = yaml.safe_load(Path(".github/dependabot.yml").read_text())
    eco = {u["package-ecosystem"]: u for u in d["updates"]}
    assert {"uv", "github-actions"} <= set(eco)
    assert eco["uv"]["schedule"]["interval"] == "weekly" and eco["uv"]["cooldown"]["default-days"] == 7
    assert "langgraph*" in eco["uv"]["groups"]["langchain"]["patterns"]
```

- [ ] **Step 2:** Run `uv run pytest tests/unit/test_ci_config.py -q`. Expected: FAIL (the files are missing).
- [ ] **Step 3: Write `ci.yml`.**
  - Triggers: `on: [push, pull_request]`. Add a concurrency group per ref.
  - Every job runs checkout, then `astral-sh/setup-uv` with `python-version`, then `uv sync --all-groups --frozen`, then a make target.
  - Resolve each action's latest release tag to its commit SHA with `gh api repos/<owner>/<repo>/git/ref/tags/<tag>`. Dereference annotated tags. Write the result as `uses: owner/repo@<sha> # <tag>`.
- [ ] **Step 4: Write `dependabot.yml`.**
  - The `uv` and `github-actions` ecosystems update weekly with `cooldown.default-days: 7`.
  - Group `langchain` has the patterns `langgraph*`, `langchain*` and `langsmith`.
- [ ] **Step 5:** Run the test file. Expected: PASS. Then commit: `ci: lint, unit and 3.15 canary jobs; dependabot`.

### Task 4: Container image and Compose base

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `compose.yaml`, `deploy/elasticmq/elasticmq.conf`
- Modify: `.github/workflows/ci.yml` (add the `docker` job)

**Interfaces:**
- Produces:
  - Image entrypoint `python -m langgraph_postgres_starter`, with default command `worker`.
  - Compose services `postgres` and `elasticmq`.
  - Queues `incidents`, `incidents-dlq` and `results`. `incidents` has a 30 s visibility timeout and redrives to `incidents-dlq` after 3 receives.

- [ ] **Step 1: Write the `Dockerfile`.**
  - Builder stage: copy the uv binary from a pinned `ghcr.io/astral-sh/uv` image. Set `UV_COMPILE_BYTECODE=1` and `UV_LINK_MODE=copy`. Run `uv sync --frozen --no-dev --no-install-project`, then copy `src/` and run `uv sync --frozen --no-dev`.
  - Runtime stage: `python:3.14-slim`. Install `ripgrep`, `git` and `ca-certificates` with apt (`--no-install-recommends`, then clean the lists).
  - Run as non-root user `app` (uid 10001). Set `ENV LANGGRAPH_STRICT_MSGPACK=true` and `STOPSIGNAL SIGTERM`.
- [ ] **Step 2:** Run `docker build -t lps:dev .`. Expected: the build succeeds.
- [ ] **Step 3:** Run `docker run --rm --entrypoint sh lps:dev -c "rg --version && git --version && python -c 'import langgraph_postgres_starter, tree_sitter_language_pack'"`. Expected: version lines, exit 0.
- [ ] **Step 4: Write `compose.yaml`.**
  - `postgres`: the image from Global Constraints, `POSTGRES_PASSWORD=postgres` (dev only), a `pg_isready` healthcheck, port 5432.
  - `elasticmq`: mounts `deploy/elasticmq/elasticmq.conf`, ports 9324 and 9325.
- [ ] **Step 5:** Run `docker compose up -d postgres elasticmq && docker compose ps`. Expected: both are running and postgres is healthy. Then `curl -s "http://localhost:9324/?Action=ListQueues"` must contain `incidents-dlq`.
- [ ] **Step 6: Add the `docker` CI job.** It builds the image only.
- [ ] **Step 7: Commit.** `build: container image and compose base services`

### Task 5: Redaction

**Files:**
- Create: `src/langgraph_postgres_starter/redaction.py`
- Test: `tests/unit/test_redaction.py`

**Interfaces:**
- Produces:
  - `REDACTED = "[REDACTED]"`
  - `def redact(text: str) -> str`
  - `def redact_obj(value: object) -> object`, which works recursively on dict, list, tuple and str values and leaves other types unchanged.

- [ ] **Step 1: Write the failing tests.** Parametrize them over the table below.

| Input | Output |
|---|---|
| `"key=AKIAIOSFODNN7EXAMPLE x"` | `"key=[REDACTED] x"` |
| `"Authorization: Bearer abc.def.ghi"` | `"Authorization: Bearer [REDACTED]"` |
| `"password=hunter2 next"` | `"password=[REDACTED] next"` |
| `"postgresql://u:s3cr3t@db/x"` | `"postgresql://u:[REDACTED]@db/x"` |
| `"mail bob@example.com now"` | `"mail [REDACTED] now"` |
| `"card 4111 1111 1111 1111"` | `"card [REDACTED]"` (Luhn-valid only) |
| `"-----BEGIN PRIVATE KEY-----\n…\n-----END PRIVATE KEY-----"` | `"[REDACTED]"` |
| `"at com.x.Foo.bar(Foo.java:42) 0x7ffd1234abcd"` | unchanged (no false positives on traces and hex addresses) |

Also add `test_redact_obj_nested`.

- [ ] **Step 2:** Run `uv run pytest tests/unit/test_redaction.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement** with compiled regexes plus a Luhn check for card numbers.
- [ ] **Step 4:** Run the tests. Expected: PASS. Commit: `feat: redaction of secrets and personal data`.

### Task 6: Retry and timeout policies

**Files:**
- Create: `src/langgraph_postgres_starter/policies.py`
- Test: `tests/unit/test_policies.py`

**Interfaces:**
- Produces:
  - `def is_transient(exc: BaseException) -> bool`
  - `def default_retry_policy() -> RetryPolicy`, with `max_attempts=3`, `initial_interval=0.5`, `backoff_factor=2.0`, `jitter=True` and `retry_on=is_transient`.
  - `def step_timeout(run_s: float) -> TimeoutPolicy`
- `is_transient` returns True for:
  - `ConnectionError`, `TimeoutError` and `langgraph.errors.NodeTimeoutError`;
  - `psycopg.errors.SerializationFailure`, `psycopg.errors.DeadlockDetected` and `psycopg.OperationalError`;
  - `aiohttp.ClientConnectionError`;
  - a `kubernetes_asyncio` `ApiException` with status 429 or ≥ 500.
- `is_transient` returns False for everything else. Examples: `botocore.exceptions.ClientError`, the botocore connection errors (botocore has already retried them), `datadog_api_client.exceptions.ApiException` (that client retries), `ValueError`, `pydantic.ValidationError` and `langgraph.errors.GraphDrained`.

- [ ] **Step 1: Write the failing test.** `test_is_transient_table` is parametrized over every case above and asserts the expected bool. `test_default_policy` asserts `p.max_attempts == 3 and p.retry_on is is_transient`.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3: Implement.** Inspect the installed `RetryPolicy` and `TimeoutPolicy` signatures before writing; Task 1 verified that both exist in `langgraph.types`.
- [ ] **Step 4:** Run it. Expected: PASS. Commit: `feat: explicit transient-error retry policy`.

### Task 7: Connection pool, resources and test database fixtures

**Files:**
- Create: `persistence/__init__.py`, `persistence/pool.py`, `persistence/resources.py`
- Modify: `tests/conftest.py`, `.github/workflows/ci.yml` (add the `integration` job, with a Postgres service and `APP_TEST_DATABASE_URL`)
- Test: `tests/integration/test_pool.py`

**Interfaces:**
- Produces:
  - `class DatabaseUnavailable(RuntimeError)`
  - `def build_conninfo(dsn: str, settings: Settings) -> str`. It adds `keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3 connect_timeout=<db_connect_timeout_s>` when they are absent.
  - `def create_pool(settings: Settings, *, dsn: str | None = None) -> AsyncConnectionPool`, with:
    - `open=False`, `min_size=db_pool_min`, `max_size=db_pool_max`, `max_lifetime=db_max_lifetime_s`;
    - `check=AsyncConnectionPool.check_connection`;
    - `kwargs={"autocommit": True, "row_factory": dict_row, "prepare_threshold": None if pgbouncer_mode else 5}`.
  - `async def open_pool(pool: AsyncConnectionPool, *, timeout: float) -> None`. It wraps `PoolTimeout` and `OperationalError` in `DatabaseUnavailable`.
  - `resources.set_pool(pool: AsyncConnectionPool | None) -> None` and `resources.current_pool() -> AsyncConnectionPool | None`.
  - Fixtures:
    - `pg_dsn` (session scope): uses `APP_TEST_DATABASE_URL` if it's set, otherwise a testcontainers `PostgresContainer("pgvector/pgvector:0.8.4-pg18")`;
    - `fresh_dsn`: creates and drops a database named `t_<uuid hex>`;
    - `pool`: an opened pool on `fresh_dsn`.

- [ ] **Step 1: Write the failing tests** (`@pytest.mark.integration`)

```python
async def test_autocommit_and_dict_rows(pool):
    async with pool.connection() as c:
        assert c.autocommit and await (await c.execute("select 1 as x")).fetchone() == {"x": 1}
async def test_killed_connection_is_replaced(pool): ...  # terminate backend via 2nd conn; next checkout works, new pid
async def test_unreachable_database_fails_fast():
    p = create_pool(load_settings(), dsn="postgresql://u:p@127.0.0.1:1/x")
    with pytest.raises(DatabaseUnavailable): await open_pool(p, timeout=3)
async def test_pgbouncer_mode_disables_prepared_statements(fresh_dsn): ...  # conn.prepare_threshold is None
def test_conninfo_adds_keepalives(): assert "keepalives=1" in build_conninfo("postgresql://u@h/db", load_settings())
```

- [ ] **Step 2:** Run `uv run pytest tests/integration/test_pool.py -m integration -q`. Expected: FAIL.
- [ ] **Step 3: Implement** the modules and the fixtures.
- [ ] **Step 4:** Run it again. Expected: PASS. Commit: `feat: health-checked connection pool and test db fixtures`.

### Task 8: Migration runner, core migration and database roles

**Files:**
- Create: `persistence/migrate.py`, `persistence/migrations/0001_action_ledger.sql`, `deploy/sql/roles.sql`
- Test: `tests/unit/test_migrate_discovery.py`, `tests/integration/test_migrate.py`

**Interfaces:**
- Consumes: `create_pool`, `open_pool` (Task 7)
- Produces:
  - `@dataclass(frozen=True) class Migration(component: str, version: int, name: str, sql: str, transactional: bool)`. `transactional` is False when the file contains `-- migrate:no-transaction`.
  - `def discover(sources: Sequence[tuple[str, Path]]) -> list[Migration]`. Files are named `NNNN_name.sql`. Order follows the source order, then the version. It raises `ValueError` on a duplicate version or a gap.
  - `async def apply(pool, migrations) -> list[Migration]`:
    - creates `schema_migrations(component, version, name, applied_at, PK(component, version))`;
    - holds `pg_advisory_lock(hashtext('lps:migrate'))` on one connection;
    - skips migrations that are already applied;
    - returns the migrations it applied.
  - `async def run_migrate(settings, *, components: set[str] | None = None, agent_sources: Sequence[tuple[str, Path]] = ()) -> list[Migration]`:
    - order: `core`, then `langgraph` (stock `AsyncPostgresSaver.setup()` on an autocommit connection), then `worker` (`worker/migrations`, if present), then each agent;
    - connects with `migration_database_url or database_url`.
  - `0001_action_ledger.sql` creates `action_ledger(key text PK, action text not null, args_hash text not null, result jsonb not null, created_at timestamptz not null default now())`.
  - `roles.sql` creates `lps_migrator` (DDL) and `lps_app` (CONNECT, USAGE, and SELECT/INSERT/UPDATE/DELETE plus default privileges). The passwords are psql variables.

- [ ] **Step 1: Write the failing tests**
  - Unit:
    - `test_discover_orders_and_validates`: core/0001, core/0002 and worker/0001 come back in that order; a duplicate raises `ValueError`; a 0001→0003 gap raises `ValueError`.
    - `test_no_transaction_marker`.
  - Integration:
    - `test_apply_is_idempotent`: the first apply returns 1 migration, the second returns `[]`.
    - `test_concurrent_apply_runs_once`: two `apply()` calls via gather apply 1 migration in total.
    - `test_langgraph_component_creates_checkpoint_tables`: `checkpoints`, `checkpoint_blobs` and `checkpoint_writes` exist.
    - `test_components_filter_skips_langgraph`.
    - `test_app_role_cannot_run_ddl`: after `roles.sql`, `lps_app` gets `InsufficientPrivilege` on `CREATE TABLE`, and `INSERT INTO action_ledger` succeeds.
- [ ] **Step 2:** Run both files. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run both files. Expected: PASS. Commit: `feat: forward-only migration runner, action_ledger, db roles`.

### Task 9: Serializer and pool-aware checkpointer

**Files:**
- Create: `persistence/serde.py`, `persistence/checkpointer.py`
- Test: `tests/unit/test_serde.py`, `tests/unit/test_upstream_shape.py`, `tests/integration/test_checkpointer.py`

**Interfaces:**
- Consumes: the `pool` fixture (Task 7)
- Produces:
  - `def make_serde(allowed_types: Sequence[type], *, aes_key: str | None = None) -> SerializerProtocol`:
    - builds `JsonPlusSerializer(pickle_fallback=False, allowed_msgpack_modules=...)`. Read the installed `JsonPlusSerializer.__init__` docstring for the exact allowlist format;
    - wraps the result in `EncryptedSerializer.from_pycryptodome_aes(...)` when `aes_key` is given.
  - `class PoolAwarePostgresSaver(AsyncPostgresSaver)`. It overrides `_cursor(self, *, pipeline: bool = False)` with the upstream body, but takes a fresh `asyncio.Lock()` on each call when `isinstance(self.conn, AsyncConnectionPool)`. This mirrors `_cursor` in `langgraph/store/postgres/aio.py`.
  - `def make_checkpointer(pool, serde, *, pgbouncer_mode: bool = False) -> PoolAwarePostgresSaver`. It sets `supports_pipeline = False` when `pgbouncer_mode` is on.

- [ ] **Step 1: Write the failing tests**

```python
def test_upstream_cursor_signature_unchanged():
    assert list(inspect.signature(AsyncPostgresSaver._cursor).parameters) == ["self", "pipeline"]
def test_upstream_still_takes_instance_lock():
    # When this fails, upstream fixed the lock: delete PoolAwarePostgresSaver (docs/practices/P3.md).
    assert "async with self.lock" in inspect.getsource(AsyncPostgresSaver._cursor)
def test_round_trip_allowed_type(): ...        # Sample(a=1) survives dumps_typed/loads_typed
def test_unknown_type_is_not_revived(): ...    # Other(b=1) comes back as a dict, not Other
def test_encrypted_round_trip_hides_plaintext(): ...  # b"12345" not in ciphertext; round-trip equal
async def test_pool_aware_saver_is_concurrent(pool): ...
    # wrap pool.connection() to sleep 50 ms while held and count simultaneous holders;
    # 8 concurrent aget_tuple: PoolAwarePostgresSaver max > 1, stock AsyncPostgresSaver max == 1
async def test_pgbouncer_mode_disables_pipeline(pool): ...  # saver.supports_pipeline is False
async def test_graph_round_trip_with_pool_aware_saver(pool): ...  # 2-node graph state persists
```

- [ ] **Step 2:** Run the tests. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run the tests. Expected: PASS. Commit: `feat: strict serde and pool-aware postgres checkpointer`.

### Task 10: Action ledger

**Files:**
- Create: `persistence/ledger.py`
- Test: `tests/integration/test_ledger.py`

**Interfaces:**
- Consumes: the `action_ledger` table (Task 8)
- Produces:
  - `@dataclass(frozen=True) class LedgerOutcome(result: dict[str, Any], replayed: bool)`
  - `class LedgerConflict(RuntimeError)`
  - `async def run_once(pool, *, key: str, action: str, args: Mapping[str, Any], fn: Callable[[], Awaitable[dict[str, Any]]]) -> LedgerOutcome`
- How `run_once` works:
  - Everything happens in one transaction that holds `pg_advisory_xact_lock(hashtextextended(key, 0))`.
  - If the row exists and `args_hash` matches, it returns `replayed=True`.
  - If `args_hash` differs, it raises `LedgerConflict`.
  - Otherwise it awaits `fn()`, inserts the row, and returns `replayed=False`.
  - `args_hash` is the SHA-256 of the args as canonical JSON (sorted keys).
  - The docstring states that an external effect is at-least-once if the process dies between `fn()` and the commit.

- [ ] **Step 1: Write the failing tests:**
  - `test_second_call_replays`: fn is called once, `out2.replayed` is true, and the results are equal.
  - `test_concurrent_same_key_runs_once`: 5 gathered calls to a slow fn call it once.
  - `test_failed_fn_records_nothing`.
  - `test_args_mismatch_raises`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: idempotent action ledger`.

### Task 11: Agent spec, registry and test-only graph

**Files:**
- Create: `agent_spec.py`, `worker/__init__.py`, `worker/registry.py`, `tests/graphs/__init__.py`, `tests/graphs/steps.py`
- Test: `tests/unit/test_registry.py`, `tests/unit/test_steps_graph.py`

**Interfaces:**
- Consumes: `default_retry_policy`, `step_timeout` (Task 6)
- Produces (`agent_spec.py`):

```python
@dataclass(frozen=True)
class RunInfo:
    run_id: UUID; dedup_key: str; attempt: int
@dataclass(frozen=True)
class AgentSpec:
    name: str
    build_graph: Callable[[], StateGraph[Any, Any, Any, Any]]
    input_model: type[BaseModel]
    dedup_key: Callable[[BaseModel, datetime], str]
    make_context: Callable[[RunInfo, BaseModel, Settings], Any]
    initial_state: Callable[[BaseModel], dict[str, Any]]
    serde_types: Sequence[type]
    migrations_dir: Path | None = None
    retention_sql: str | None = None   # parameter %(cutoff)s
```

- Produces (`worker/registry.py`):
  - `def builtin_agents() -> list[AgentSpec]`. It returns `[]` for now; Task 33 adds one line.
  - `def load_agents(settings: Settings, *, include: Sequence[AgentSpec] = ()) -> dict[str, AgentSpec]`. This adds the built-in agents, then `include`, then each `"module:attr"` entry in `settings.extra_agents`.
- Produces (`tests/graphs/steps.py`):
  - `STEPS_SPEC` (name `test_steps`).
  - Input `StepsInput(key: str)`.
  - State `log: Annotated[list[str], operator.add]`.
  - Async nodes `step_a`, `step_b` and `step_c`:
    - each records `"<step>:start"` and `"<step>:done"`, either to the table `test_step_calls(run_id text, mark text, at timestamptz)` (when `current_pool()` is set) or to an in-memory list;
    - each reads faults from the module-level `FAULTS: dict[str, Fault]`, keyed by `key`. A fault has `fail_step`, `kind` (`transient` or `fatal`), `times` and `delay_s: dict[str, float]`.
  - The graph uses `set_node_defaults(retry_policy=default_retry_policy(), timeout=step_timeout(30))`.
  - `async def ensure_table(pool) -> None` creates `test_step_calls`. The integration and e2e fixtures call it.

- [ ] **Step 1: Write the failing tests:**
  - `test_load_agents_reads_extra_agents`: setting `APP_EXTRA_AGENTS=["tests.graphs.steps:STEPS_SPEC"]` puts `"test_steps"` in the result.
  - `test_steps_graph_runs`: with an `InMemorySaver`, `log == ["a","b","c"]`.
  - `test_transient_failure_is_retried`: `b:start` appears twice and the run completes.
  - `test_fatal_failure_is_not_retried`: the call raises, and `b:start` appears once.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: agent spec, registry and test-only steps graph`.

### Task 12: Runs table and repository

**Files:**
- Create: `worker/migrations/0001_runs.sql`, `worker/runs.py`
- Test: `tests/integration/test_runs_repo.py`

**Interfaces:**
- Produces:
  - `class RunStatus(StrEnum)` with `PENDING`, `RUNNING`, `SUCCESS` and `FAILED`. The spec's `error` status is not needed, because errors return a run to `pending` or `failed` (§8.2).
  - `@dataclass(frozen=True) class RunRow` with `run_id`, `agent`, `dedup_key`, `status`, `input: dict`, `attempt`, `lease_owner`, `lease_expires_at`, `created_at`, `started_at`, `ended_at`, `error: dict | None` and `checkpoints_pruned_at`.
  - `class RunRepo(pool)`, with these async methods:
    - `create(agent, dedup_key, input) -> tuple[UUID, bool]`
    - `claim(run_id, owner, lease_s) -> RunRow | None`
    - `claim_recoverable(owner, lease_s, limit) -> list[RunRow]`. It takes runs that are pending, or running with `lease_expires_at < now()`, with `FOR UPDATE SKIP LOCKED`.
    - `heartbeat(owner, run_ids, lease_s) -> int`
    - `mark_success(run_id, owner) -> bool`
    - `mark_error(run_id, owner, error: dict, max_attempts) -> RunStatus`
    - `release(run_ids, owner) -> int`
    - `get(run_id) -> RunRow | None`
    - `list(status: RunStatus | None = None, limit: int = 50) -> list[RunRow]`
    - `retry(run_id) -> bool`
- SQL: the columns in §7.3 plus `checkpoints_pruned_at timestamptz`, a unique index on `dedup_key`, an index on `(status, lease_expires_at)`, and a `check` constraint on status.

- [ ] **Step 1: Write the failing tests:**
  - `test_create_is_idempotent_on_dedup_key`: the calls return `(id, True)` and then `(id, False)`.
  - `test_claim_recoverable_never_double_claims`: 10 pending runs; two gathered claims with different owners return disjoint sets whose union has 10 runs.
  - `test_expired_lease_is_reclaimed`: a lease of 1 s, a sleep of 1.1 s, and the new owner is set.
  - `test_live_lease_is_not_reclaimed`.
  - `test_heartbeat_extends_only_own_runs`.
  - `test_mark_error_pending_then_failed`: with `max_attempts=3`, the run goes pending, pending, failed.
  - `test_release_clears_lease`.
  - `test_retry_resets_failed`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: runs table with leases and recoverable claims`.

### Task 13: Run executor

**Files:**
- Create: `worker/executor.py`
- Test: `tests/integration/test_executor.py`

**Interfaces:**
- Consumes: `AgentSpec`, `RunInfo` (Task 11), `RunRepo`, `RunRow`, `RunStatus` (Task 12), `make_checkpointer` (Task 9), `redact` (Task 5)
- Produces:
  - `class RunExecutor(*, saver, agents: Mapping[str, AgentSpec], repo: RunRepo, settings: Settings, owner: str, metadata: Callable[[RunRow], dict[str, str]] | None = None)`
  - `async def execute(self, run: RunRow) -> RunStatus`
  - `def request_drain_all(self, reason: str = "shutdown") -> int`
  - `def active_run_ids(self) -> list[UUID]`
- How `execute` works:
  - It compiles each agent's graph once, with `saver`.
  - Config: `{"configurable": {"thread_id": str(run_id)}, "metadata": metadata(run) or {}}`.
  - Input: `None` if `await saver.aget_tuple(config)` returns a checkpoint, otherwise `spec.initial_state(model)`.
  - It calls `ainvoke(..., context=spec.make_context(RunInfo(...), model, settings), durability="sync", control=RunControl())` inside `asyncio.timeout(settings.run_deadline_seconds)`.
  - On `GraphDrained`, it calls `repo.release` and returns `PENDING`.
  - On any other exception, it calls `repo.mark_error` with `{"type": ..., "message": redact(str(exc))[:2000]}`.
  - On success, it calls `repo.mark_success`.

- [ ] **Step 1: Write the failing tests.** These use `STEPS_SPEC` with `set_pool(pool)`.
  - `test_run_completes`: status `SUCCESS`, and the checkpoint `log == ["a","b","c"]`.
  - `test_resume_skips_completed_steps`: a fatal fault in b makes the first execute return `PENDING`. After the fault is cleared, the second execute returns `SUCCESS`. Afterwards `a:done` appears once, `b:start` twice and `c:done` once.
  - `test_deadline_exceeded_returns_pending_with_error`: deadline 1 s and a delay of 5 s in b. The result is `error["type"] == "TimeoutError"`.
  - `test_drain_releases_after_current_step`: a delay of 1 s in b; drain is requested while b runs. The result is `PENDING`, `b:done` appears once, and the next execute runs only c.
  - `test_configurable_holds_only_thread_id`: spy on `ainvoke`; `config["configurable"].keys() == {"thread_id"}` (P14).
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: run executor with deadline, drain and resume`.

### Task 14: SQS intake

**Files:**
- Create: `worker/intake.py`
- Modify: `tests/conftest.py` (add an `sqs` fixture: ElasticMQ via testcontainers `DockerContainer("softwaremill/elasticmq-native:1.7.1")`, or `APP_TEST_SQS_ENDPOINT`)
- Test: `tests/integration/test_intake.py`

**Interfaces:**
- Consumes: `RunRepo` (Task 12), `AgentSpec` (Task 11)
- Produces:
  - `def make_sqs_client(settings: Settings) -> Any`. It creates a boto3 `sqs` client with `region_name` and `endpoint_url`.
  - `class SqsIntake(*, client, queue_url: str, agent: AgentSpec, repo: RunRepo, wait_seconds: int)`, with:
    - `async def poll(self, max_messages: int) -> list[UUID]`, which returns the IDs of new runs;
    - `last_poll_monotonic: float`.
- How `poll` works:
  - It calls `receive_message` in `asyncio.to_thread`, with `MaxNumberOfMessages=min(max_messages, 10)`, `WaitTimeSeconds=wait_seconds`, and `MessageSystemAttributeNames=["ApproximateReceiveCount", "SentTimestamp"]`.
  - It parses each body as JSON and calls `agent.input_model.model_validate`.
  - If a message is invalid, it logs `intake.invalid` and doesn't delete the message.
  - If it's valid, it computes `dedup = agent.dedup_key(model, sent_at)`, calls `repo.create`, deletes the message, and logs `intake.duplicate` when no run was created.

- [ ] **Step 1: Write the failing tests.** Each test creates its own queues with `create_queue`.
  - `test_valid_message_creates_run_and_is_deleted`.
  - `test_duplicate_message_creates_one_run`.
  - `test_invalid_message_reaches_dlq`: the queue has `VisibilityTimeout=1` and a redrive policy with `maxReceiveCount` 3. After three polls with sleeps of 1.1 s, there are 0 runs and the DLQ holds 1 message.
  - `test_poll_respects_max_messages`: 5 messages are sent, and `poll(2)` returns 2.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: SQS intake with dedup and DLQ-safe validation`.

### Task 15: Health server and CLI

**Files:**
- Create: `worker/health.py`, `worker/cli.py`
- Modify: `__main__.py`
- Test: `tests/unit/test_health.py`, `tests/integration/test_cli.py`

**Interfaces:**
- Produces:
  - `class ReadinessProbe(pool, *, intake_alive: Callable[[], bool])`, with `async def check(self) -> tuple[bool, dict[str, bool]]`. The checks are `database` (`SELECT 1` within 2 s), `pool_open` and `intake`.
  - `def create_health_app(probe: ReadinessProbe) -> FastAPI`:
    - `GET /ok` returns 200 with `{"status": "ok"}`;
    - `GET /ready` returns 200 or 503 with `{"ready": bool, "checks": {...}}`.
  - CLI subcommands of `python -m langgraph_postgres_starter`:
    - `worker`
    - `migrate [--components a,b]`
    - `runs list [--status S] [--limit N]`
    - `runs show RUN_ID`
    - `runs retry RUN_ID`

    Output is JSON lines, and the exit code is 0 on success. `migrate` passes `agent_sources=[(s.name, s.migrations_dir) for s in load_agents(settings).values() if s.migrations_dir]` to `run_migrate`.

- [ ] **Step 1: Write the failing tests:**
  - `test_ok_always_200`.
  - `test_ready_503_when_database_down`: the response has `checks["database"] is False` (P5).
  - `test_ready_200_when_all_ok`.
  - `test_cli_runs_show_prints_json`.
  - `test_cli_migrate_components_core`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.** The `worker` subcommand calls `worker.main.main` (Task 16); until then it raises `SystemExit("worker not available")`.
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: health endpoints and operator CLI`.

### Task 16: Worker main loop: recovery, heartbeats and drain

**Files:**
- Create: `worker/main.py`
- Test: `tests/integration/test_worker_loop.py`

**Interfaces:**
- Consumes: Tasks 7, 9, 12–15
- Produces:
  - `class Worker(settings: Settings, *, agents: Mapping[str, AgentSpec], sqs_client: Any | None, owner: str)`
  - `async def start(self) -> None`
  - `async def stop(self) -> None`
  - `def main(argv: Sequence[str] | None = None) -> int`
- What `start` does:
  - It opens the pool with `timeout=db_connect_timeout_s`. If that fails, startup fails.
  - It calls `set_pool`, then builds the serde from the union of `serde_types` and the checkpointer.
  - When `sqs_queue_url` is set, it builds an `SqsIntake` for `agents[settings.intake_agent]`.
  - It starts the uvicorn health server on `health_port`.
  - It runs these loops:
    - intake, polling only when there are free slots below `worker_concurrency`;
    - recovery, once immediately and then every `recovery_interval_seconds`, calling `claim_recoverable(owner, lease_seconds, free_slots)`;
    - heartbeat, every `heartbeat_seconds`;
    - pruner, hourly (Task 17).
- What `stop` does, in order:
  1. Stops intake.
  2. Calls `request_drain_all()`.
  3. Waits up to `drain_timeout_seconds`.
  4. Releases any runs that are still active.
  5. Closes the pool and flushes tracing.
- `main` installs SIGTERM and SIGINT handlers that call `stop`. The owner is the `POD_NAME` environment variable, or the hostname.

- [ ] **Step 1: Write the failing tests.** These run in-process with Postgres, ElasticMQ and `STEPS_SPEC`.
  - `test_processes_queued_message`: the run reaches `SUCCESS` within 10 s.
  - `test_recovery_resumes_expired_run`: prepare a run with a checkpoint after a, owned by `dead-pod` with an expired lease. The worker brings it to `SUCCESS`, and `a:done` appears once.
  - `test_stop_drains_and_releases`: a delay of 2 s in b. `stop()` returns within the drain timeout, and the run is `pending` with no lease.
  - `test_concurrency_limit`: with concurrency 2 and 5 slow messages, no more than 2 runs are ever running at once.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: worker loop with recovery, heartbeats and graceful drain`.

### Task 17: Retention pruner

**Files:**
- Create: `worker/retention.py`, `worker/migrations/0002_runs_pruned_at.sql` (only if Task 12 didn't create the column)
- Modify: `worker/main.py` (the hourly loop)
- Test: `tests/integration/test_retention.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class PruneReport(leader: bool, threads_pruned: int, rows_deleted: int)`
  - `async def prune_once(pool, saver, agents: Mapping[str, AgentSpec], settings: Settings, *, now: datetime) -> PruneReport`
- How `prune_once` works:
  - It tries `pg_try_advisory_lock(hashtext('lps:prune'))` on a dedicated connection. If another worker holds it, it returns `PruneReport(False, 0, 0)`.
  - It looks for runs with status `success` or `failed`, `ended_at < now - retention_checkpoint_days` and `checkpoints_pruned_at is null`, in batches of 100. For each batch it calls `await saver.prune([str(id)...], strategy="delete")` and sets `checkpoints_pruned_at`.
  - It deletes `runs` rows with `ended_at < now - retention_result_days`, and runs each agent's `retention_sql` with `cutoff`.
  - It retries `SerializationFailure` and `DeadlockDetected` up to 3 times.

- [ ] **Step 1: Write the failing tests:**
  - `test_prunes_only_old_finished_runs`: the run that ended 20 days ago is pruned; the run that ended 1 day ago keeps its checkpoints.
  - `test_deletes_old_rows_and_runs_agent_sql`.
  - `test_single_leader`: two gathered calls give exactly one `leader=True`.
  - `test_running_runs_never_pruned`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: leader-elected retention pruner`.

### Task 18: Durability end-to-end tests and Compose worker services

**Files:**
- Create: `tests/e2e/helpers.py`, `tests/e2e/test_durability.py`
- Modify: `compose.yaml`, `Makefile`, `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `python -m langgraph_postgres_starter worker` (Task 16)
- Produces:
  - Compose services:
    - `migrate`: `command: ["migrate"]`, depends on postgres being healthy, `restart: "no"`;
    - `worker`: depends on migrate having completed successfully. Its environment sets `APP_ADAPTERS=simulated`, `APP_LLM_MODE=recorded`, `APP_SQS_ENDPOINT_URL=http://elasticmq:9324` and `APP_SQS_QUEUE_URL=http://elasticmq:9324/000000000000/incidents`, plus dummy AWS keys and `APP_DATABASE_URL`. Its healthcheck calls `/ready`, and `stop_grace_period` is 60 s.
  - Make targets `up`, `down`, `migrate`, `test-int` and `test-e2e`.
  - The CI job `e2e`.
  - Helpers:
    - `start_worker(env) -> subprocess.Popen`
    - `wait_for(pred, timeout)`
    - `marks(run_id) -> list[str]`, read from `test_step_calls`

- [ ] **Step 1: Write the failing tests** (`@pytest.mark.e2e`; each starts the worker with `APP_EXTRA_AGENTS=["tests.graphs.steps:STEPS_SPEC"]`)
  - `test_kill_9_resumes_at_failed_step`, which proves P11:
    1. b has a 5 s delay.
    2. When `b:start` is seen, send SIGKILL to worker A.
    3. Start worker B and wait for `SUCCESS`.
    4. Assert that `a:done` appears once, `b:start` twice and `c:done` once.
  - `test_sigterm_drains_then_resumes`, which proves P10:
    1. b has a 3 s delay.
    2. Send SIGTERM to A at `b:start`. A must exit within 50 s with code 0.
    3. The run must be `pending`.
    4. Start B and wait for `SUCCESS`.
    5. Assert that `b:done` appears once and `c:done` once.
- [ ] **Step 2:** Run `uv run pytest tests/e2e/test_durability.py -m e2e -q`. Expected: FAIL.
- [ ] **Step 3: Implement** the helpers and the Compose/Makefile/CI changes.
- [ ] **Step 4:** Run the tests. Expected: PASS. Then run `docker compose up -d --build`. Expected: `migrate` exits 0 and `worker` becomes healthy.
- [ ] **Step 5: Commit.** `test: kill -9 and SIGTERM durability e2e; compose worker`

### Task 19: LangSmith tracing and run metadata

**Files:**
- Create: `observability/__init__.py`, `observability/langsmith.py`, `observability/metadata.py`
- Modify: `worker/main.py` (configure at start, flush at stop), `worker/executor.py` (pass `metadata=run_metadata`)
- Test: `tests/unit/test_tracing.py`

**Interfaces:**
- Produces:
  - `def configure_tracing(settings: Settings) -> langsmith.Client | None`:
    - returns None unless `LANGSMITH_TRACING` is true and `LANGSMITH_API_KEY` is set;
    - otherwise builds a `Client` with `anonymizer` (built from `redact`), `hide_inputs`/`hide_outputs` from `content_hooks(settings.trace_content)`, `tracing_sampling_rate` and `tracing_error_callback`, then makes it the default client for traces. The builder checks the supported mechanism in the installed langsmith.
  - `def content_hooks(mode) -> tuple[Callable[[dict], dict], Callable[[dict], dict]]`:
    - `full` changes nothing;
    - `redacted` applies `redact_obj`;
    - `none` returns `{}`.
  - `def trace_llm(name: str) -> Callable[[F], F]`. It wraps `langsmith.traceable(run_type="llm", name=name)` and adds `model_id` and `usage` from the returned `LlmResponse` to the metadata.
  - `def run_metadata(run: RunRow, settings: Settings) -> dict[str, str]`. Its keys are `run_id`, `dedup_key`, `agent`, `cluster`, `env`, `app_version` and `git_sha`. It also adds `namespace` and `workload` when `run.input` has them. The analyze trace (Task 31) records the model ID, and the outputs record the category.

- [ ] **Step 1: Write the failing tests:**
  - `test_tracing_off_by_default`.
  - `test_content_hooks[full|redacted|none]`: with input `{"log": "password=hunter2 boom"}`, the output is unchanged for `full`, `"password=[REDACTED] boom"` for `redacted`, and `{}` for `none`.
  - `test_metadata_has_no_secrets`: with `dd_api_key="SECRET"`, the string is not in `json.dumps(md)`.
  - `test_trace_llm_records_usage`: uses a fake client or a patched `traceable`, with no network.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat: langsmith tracing with content modes and run metadata`.

### Task 20: Agent schemas, state and dedup key

**Files:**
- Create: `agents/__init__.py`, `agents/incident_triage/__init__.py`, `agents/incident_triage/{schemas.py,state.py,dedup.py}`
- Test: `agents/incident_triage/tests/test_schemas.py`

**Interfaces:**
- Produces (`schemas.py`, Pydantic v2, `extra="forbid"`):
  - `WorkloadRef(kind: Literal["Deployment","StatefulSet","DaemonSet","Job"], name: str)`.
  - `IncidentMessage`, with the fields in §9.1, a validator that requires `pod` or `workload`, and `check_cluster(expected: str) -> None`.
  - `ImageRef(container, image, digest: str | None)`.
  - `Target(cluster, namespace, workload: WorkloadRef | None, pod: str | None, container: str | None, node: str | None, images: list[ImageRef])`.
  - `EvidenceItem(id, kind, title, text, meta: dict[str, str])`:
    - `id` matches `^(k8s|log|dd|code|note):[a-z_]+:[A-Za-z0-9._/-]+$`;
    - `kind` is one of `k8s_status`, `k8s_event`, `log`, `node`, `datadog_event` or `note`;
    - `text` is capped by `MAX_EVIDENCE_TEXT = 4000`.
  - `Frame(file, line: int, function: str | None, is_app_frame: bool, family: str)`.
  - `CodeSnippet(id, repo, commit, path, start_line, end_line, symbol: str | None, language, text)`.
  - `Assessment(collect_code: bool, infra_signals: list[str], app_signals: list[str], reasons: list[str])`.
  - `EvidenceCitation(id, why)` and `CodeCitation(snippet_id, path, start_line, end_line, symbol: str | None)`.
  - `TriageAnalysis`, with the fields and caps in §9.8. A model validator rejects an empty `evidence` list unless `category == "unknown"`.
  - `Usage(input_tokens: int, output_tokens: int, cache_read_input_tokens: int = 0)`.
  - `TriageResult`, with the fields in §9.8.
  - `SERDE_TYPES: tuple[type, ...]`, every model that is stored in state.
- Produces (`state.py`): `class TriageState(TypedDict, total=False)` with:
  - `message: IncidentMessage`
  - `target: Target`
  - `k8s_evidence: list[EvidenceItem]`
  - `log_text: str`
  - `datadog_evidence: list[EvidenceItem]`
  - `assessment: Assessment`
  - `code_evidence: list[CodeSnippet]`
  - `notes: Annotated[list[EvidenceItem], operator.add]`
  - `analysis: TriageAnalysis | None`
  - `refusal: bool`
  - `usage: Usage | None`
  - `model_id: str | None`
  - `published: list[str]`
- Produces (`dedup.py`): `def dedup_key(msg: IncidentMessage, received_at: datetime) -> str`. It's the SHA-256 hex of `"|".join([cluster or "", namespace, workload or pod, alert_id or 15-minute bucket of (fired_at or received_at)])`.

- [ ] **Step 1: Write the failing tests:**
  - `test_pod_or_workload_required`.
  - `test_check_cluster_mismatch_raises`.
  - `test_dedup_same_15_min_bucket`: 10:01 and 10:14 match; 10:16 differs.
  - `test_dedup_uses_alert_id`.
  - `test_analysis_caps`: a 601-character summary fails, 6 actions fail, `application` with no evidence fails, and `unknown` with no evidence passes.
  - `test_state_types_round_trip_strict_serde`: one sample of each type in `SERDE_TYPES` goes through `make_serde(SERDE_TYPES)` and comes back equal.
- [ ] **Step 2:** Run `uv run pytest src/langgraph_postgres_starter/agents/incident_triage/tests/test_schemas.py -q`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it again. Expected: PASS. Commit: `feat(triage): schemas, state and dedup key`.

### Task 21: Adapter interfaces, fakes, context and fixture loader

**Files:**
- Create:
  - `agents/incident_triage/adapters/__init__.py`
  - `adapters/{k8s,datadog,source,llm,sinks}/{__init__.py,interface.py}`
  - `adapters/k8s/fake.py`, `adapters/datadog/fake.py`, `adapters/source/local.py`, `adapters/llm/recorded.py`, `adapters/sinks/log.py`
  - `agents/incident_triage/context.py`, `agents/incident_triage/fixtures/loader.py`, `agents/incident_triage/fixtures/FORMAT.md`
- Test: `agents/incident_triage/tests/test_adapters.py`

**Interfaces:**
- Produces (Protocols, all async):
  - `K8sAdapter`:
    - `get_pod(ns, name) -> PodInfo | None`
    - `list_pods_for_workload(ns, wl: WorkloadRef) -> list[PodInfo]`
    - `get_owner_workload(ns, pod) -> WorkloadRef | None`
    - `list_events(ns, kind, name) -> list[K8sEvent]`
    - `read_logs(ns, pod, container, *, previous: bool, tail_lines: int) -> str | None`
    - `get_node_conditions(node) -> list[NodeCondition]`
    - `get_workload_annotations(ns, wl) -> dict[str, str]`
  - Dataclasses:
    - `PodInfo(name, namespace, node, phase, containers: list[ContainerStatus], annotations, owner: WorkloadRef | None)`
    - `ContainerStatus(name, image, image_id, restart_count, state, waiting_reason, last_terminated: Terminated | None, ready)`
    - `Terminated(reason, exit_code, signal)`
    - `K8sEvent(reason, message, type, count, last_seen, involved_kind, involved_name)`
    - `NodeCondition(type, status, reason)`
  - `DatadogAdapter.search_events(query: str, start: datetime, end: datetime, limit: int) -> list[DdEvent]`, where `DdEvent(id, title, text, timestamp, tags: list[str], source)`.
  - `SourceLocator.locate(target: Target, annotations: Mapping[str, str]) -> SourceRef | None`, where `SourceRef(repo_url, commit, origin: Literal["annotation","image_label","config","fixture"])`.
  - `SourceProvider.checkout(ref: SourceRef) -> Path`.
  - `LlmAdapter.analyze(req: AnalysisRequest) -> LlmResponse`:
    - `AnalysisRequest(system: str, user: str, schema: dict, fixture_id: str | None, attempt: int, prompt_version: str)`. `RecordedLlm` reads `fixtures/recorded/<fixture_id>/<prompt_version>/attempt-<attempt>.json`.
    - `LlmResponse(text: str, stop_reason: str, model_id: str, usage: Usage)`
  - `ResultSink`, with `name: str` and `send(result: TriageResult, idempotency_key: str) -> dict[str, Any]`. `LogSink` (`name = "log"`) writes one log event, `triage.published`, carrying the result JSON and the key.
  - `@dataclass(frozen=True) class Adapters(k8s, datadog, locator, source, llm, sinks: tuple[ResultSink, ...])`.
  - `def build_adapters(settings: Settings, *, fixture_id: str | None) -> Adapters`. In simulated mode it builds the fakes from the fixture. In real mode, each adapter not built yet raises `NotImplementedError` naming the task that adds it (23, 24, 28, 31, 32).
- Produces (fakes and loader):
  - Each fake appends `"<adapter>.<method>[:start]"` lines to `settings.fake_call_log_dir / f"{run_id}.log"` when that setting is set.
  - `FakeDatadog` sleeps `settings.fake_datadog_delay_s` after logging `datadog.search_events:start`.
  - `context.py`:
    - `@dataclass(frozen=True) class TriageContext(run: RunInfo, settings: Settings, adapters: Adapters, fixture_id: str | None)`
    - `def make_context(info: RunInfo, msg: IncidentMessage, settings: Settings) -> TriageContext`. The fixture ID is `msg.raw["fixture_id"]` in simulated mode.
    - `def default_context() -> TriageContext`, used by Studio with `settings.studio_fixture`.
    - `def ctx(runtime: Runtime[TriageContext]) -> TriageContext`, which returns `runtime.context` or `default_context()`.
  - `fixtures/loader.py`:
    - `@dataclass(frozen=True) class Fixture(id, incident: dict, k8s: dict, datadog: dict, expected: dict, source: dict | None)`
    - `def load_fixture(fixture_id: str, root: Path | None = None) -> Fixture`
    - `def list_fixtures(root: Path | None = None) -> list[str]`. Names that start with `_` are excluded.
  - The fixture file format is written in `FORMAT.md`, exactly as in Task 22.

- [ ] **Step 1: Write the failing tests.** They use a tiny `tests/fixtures/_sample` fixture written in the test with `tmp_path`.
  - `test_fake_k8s_returns_fixture_pod`.
  - `test_fake_call_log_written`.
  - `test_recorded_llm_missing_reply_raises_clear_error`: the error message names the expected path.
  - `test_real_mode_not_ready_names_task`.
  - `test_default_context_uses_studio_fixture`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): adapter interfaces, fakes, context, fixture loader`.

### Task 22: Fixture apps and incidents

**Files:**
- Create:
  - `fixtures/apps/{javascript-node,typescript-node,python,java-spring,java-plain,go,php,ruby,kotlin,scala,c,cpp}/…`
  - `fixtures/incidents/<id>/{incident.json,k8s.json,datadog.json,expected.json[,source.json]}` for these 16 IDs: `js-typeerror`, `ts-undefined-property`, `python-keyerror`, `java-spring-npe`, `java-plain-arithmetic`, `go-nil-map`, `php-division-by-zero`, `ruby-nomethod`, `kotlin-illegal-state`, `scala-match-error`, `c-segfault`, `cpp-out-of-range`, `infra-oomkilled`, `infra-imagepull`, `infra-probe-failure`, `infra-config-error`
- Test: `agents/incident_triage/tests/test_fixtures_valid.py`

**Interfaces (the file format, also written in `FORMAT.md`):**
- `incident.json` is an `IncidentMessage` without `fired_at`, with `raw.fixture_id = <id>`.
- `k8s.json` holds `{"pods": [PodInfo…], "events": [K8sEvent…], "logs": {"<pod>/<container>/current": str|null, "<pod>/<container>/previous": str|null}, "nodes": {"<node>": [NodeCondition…]}, "annotations": {}}`.
- `datadog.json` holds `{"events": [DdEvent…]}`.
- `expected.json` holds `{"category": str, "code": {"path", "start_line", "end_line", "symbol"} | null, "facts": [str]}`.
- `source.json` holds `{"app": "<apps dir>", "commit": "fixture"}`.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.parametrize("fid", list_fixtures())
def test_fixture_valid(fid):
    f = load_fixture(fid); IncidentMessage.model_validate(f.incident)
    assert f.expected["category"] in {"infrastructure","configuration","application","dependency","unknown"}
    if f.expected["code"]:
        src = settings.fixtures_dir / "apps" / f.source["app"] / f.expected["code"]["path"]
        assert src.exists() and f.expected["code"]["end_line"] <= len(src.read_text().splitlines())
def test_every_language_has_an_app():
    assert {language_of(app) for app in apps()} >= {"javascript","typescript","python","java","go","php","ruby","kotlin","scala","c","cpp"}
def test_app_crash_logs_point_at_bug_line(): ...  # each app fixture's previous log names expected path suffix and a line in range
```

- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Write the fixtures.**
  - **Apps:** each has 1–4 source files and at most 150 lines in total, laid out the way a real app would be. Examples:
    - `java-spring`: `src/main/java/com/example/orders/OrderController.java`
    - `go`: `cmd/api/main.go` plus `internal/orders/service.go`
    - `typescript-node`: `src/orders/service.ts`. Its log frames point at `/app/dist/orders/service.js`.
  - **Crash logs:** each `previous` log reproduces the runtime's real trace format, with container paths (`/app`, `/usr/src/app`, `/var/www/html`, `/workspace`) and the real bug line number.
    - `cpp-out-of-range` has a symbolized backtrace.
    - `c-segfault` has no trace. It has only `Segmentation fault (core dumped)` after the log line `processing order batch for warehouse` (a literal in its source).
  - **Infrastructure incidents** carry the status and events for their reason (§9.5) and have no app.
- [ ] **Step 4:** Run the tests. Expected: PASS. Commit: `test(triage): 12 sample apps and 16 incident fixtures`.

### Task 23: Resolve target and Kubernetes evidence (with the real adapter)

**Files:**
- Create: `agents/incident_triage/evidence.py`, `nodes/__init__.py`, `nodes/resolve.py`, `nodes/k8s.py`, `adapters/k8s/real.py`
- Modify: `adapters/__init__.py` (wire `RealK8s`)
- Test: `tests/test_resolve.py`, `tests/test_k8s_node.py`, `tests/test_k8s_real.py` (all under `agents/incident_triage/`)

**Interfaces:**
- Consumes: Tasks 20–22
- Produces (`evidence.py`):
  - `def make_id(kind: str, sub: str, key: str) -> str`
  - `def cap_text(text: str, max_chars: int = MAX_EVIDENCE_TEXT) -> str`
  - `def decode_log(raw: bytes | str, *, max_line: int = 2000) -> str`. It decodes UTF-8 with replacement and cuts longer lines with `"…[truncated]"`.
  - `def number_lines(text: str, start: int = 1) -> str`
- Produces (nodes):
  - `async def resolve_target(state, runtime) -> dict`. It returns `{"target": Target, "notes": [...]}` after running `check_cluster`. Container choice: the message's container; otherwise a non-sidecar container with a non-zero last termination; otherwise the most restarts. The sidecars are `istio-proxy`, `linkerd-proxy`, `datadog-agent`, `envoy`, `vault-agent`, plus `cloud-sql-proxy`.
  - `async def collect_k8s(state, runtime) -> dict`. It returns `{"k8s_evidence": [...], "log_text": str}`.
- Evidence IDs and `meta` keys, which assess (Task 27) relies on:
  - `k8s:status:<container>`: `state`, `reason`, `waiting_reason`, `exit_code`, `signal`, `restart_count`
  - `k8s:event:<n>`: `reason`, `type`
  - `k8s:node:<node>`: `type`, `status`
  - `log:current:<container>` and `log:previous:<container>`: line-numbered tails of at most `log_tail_lines` lines
  - `log_text` is the previous log if it exists, otherwise the current log, after `redact`.
- Produces (`RealK8s(K8sAdapter)`):
  - It uses `kubernetes_asyncio`, with `load_incluster_config()` and a fallback to `load_kube_config()`.
  - `read_logs(previous=True)` returns None on `ApiException` 400.

- [ ] **Step 1: Write the failing tests:**
  - `test_resolve_pod_to_owner_workload` (`python-keyerror`: Deployment `orders`).
  - `test_resolve_workload_to_failing_pod`.
  - `test_resolve_pod_gone_falls_back_to_workload` (Review Focus 1).
  - `test_resolve_nothing_found_continues`: notes include `note:target:not_found` (Review Focus 1).
  - `test_pick_container_ignores_healthy_sidecar` (Review Focus 2).
  - `test_collect_k8s_ids_unique_and_tail_capped`.
  - `test_previous_logs_unavailable_is_a_note` (Review Focus 3).
  - `test_decode_log_bytes_and_long_lines`: `b"\xff\xfeboom"` contains `"boom"`; a 10,000-character line is cut to 2000 plus the marker.
  - `test_evidence_is_redacted`: a log line with `password=x` is stored as `[REDACTED]`.
  - `test_real_previous_log_400_returns_none`: a mocked `CoreV1Api` raises `ApiException(status=400)`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): resolve target and collect kubernetes evidence`.

### Task 24: Datadog evidence (with the real adapter)

**Files:**
- Create: `nodes/datadog.py`, `adapters/datadog/real.py`
- Modify: `adapters/__init__.py`
- Test: `tests/test_datadog_node.py`, `tests/test_datadog_real.py`

**Interfaces:**
- Produces:
  - `def build_query(target: Target) -> str`. For example: `kube_namespace:<ns> (pod_name:<pod> OR kube_deployment:<name>)`. The builder confirms the Datadog event-search syntax in its docs and records the URL in the `build_query` docstring.
  - `def window(fired_at: datetime, lookback_min: int) -> tuple[datetime, datetime]`. It returns `(fired_at - lookback, fired_at + 5 min)`.
  - `async def collect_datadog(state, runtime) -> dict`. It returns `{"datadog_evidence": [...]}`, with IDs `dd:event:<id>`, capped at `dd_max_events`, after `redact`.
  - `RealDatadog(DatadogAdapter)`. It uses `AsyncApiClient(Configuration(...))` with `enable_retry=True` and the `site`, and calls `EventsApi.search_events` with a filter (query, from, to) and a page limit. Verify the class names in the installed client.

- [ ] **Step 1: Write the failing tests:**
  - `test_query_for_pod_and_workload`.
  - `test_window_bounds`.
  - `test_events_capped_and_ids`.
  - `test_real_adapter_builds_request`: a mocked `EventsApi` captures the request body.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): datadog events evidence`.

### Task 25: Stack-trace parsers I (JVM, Node, Python)

**Files:**
- Create: `codeintel/__init__.py`, `codeintel/stacktrace/{__init__.py,jvm.py,node.py,python.py}`
- Test: `tests/test_stacktrace_jvm_node_python.py`

**Interfaces:**
- Produces:
  - `class Family(StrEnum)`: `JVM`, `NODE`, `PYTHON`, `GO`, `PHP`, `RUBY`, `NATIVE`.
  - `def detect_families(text: str) -> set[Family]`.
  - `def parse_frames(text: str) -> list[Frame]`. It runs every detected family's parser and removes duplicate frames.
  - Each family module has `def parse(text: str) -> list[Frame]`.
  - **Frame order:** innermost first. Python tracebacks are reversed to get this. For JVM traces, the deepest `Caused by:` block comes first.
- What each parser treats as a library frame:

  | Family | Library frames |
  |---|---|
  | JVM | prefixes `java.`, `javax.`, `jdk.`, `sun.`, `kotlin.`, `kotlinx.`, `scala.`, `akka.`, `org.springframework.`, `org.apache.`; and `Native Method` |
  | Node | `node:internal`, `node_modules` |
  | Python | `site-packages`, `dist-packages`, `<frozen`, `/usr/lib/python3`, `/usr/local/lib/python3.` |

  JVM file paths are built from the package (`com/example/orders/OrderController.java`).

- [ ] **Step 1: Write the failing tests.** For each fixture's crash log (`js-typeerror`, `ts-undefined-property`, `python-keyerror`, `java-spring-npe`, `java-plain-arithmetic`, `kotlin-illegal-state`, `scala-match-error`), the first app frame's file ends with the expected path suffix and its line equals `expected.code` start…end. Also add `test_library_frames_flagged`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): JVM, Node and Python stack-trace parsers`.

### Task 26: Stack-trace parsers II (Go, PHP, Ruby, native)

**Files:**
- Create: `codeintel/stacktrace/{go.py,php.py,ruby.py,native.py}`
- Test: `tests/test_stacktrace_go_php_ruby_native.py`

**Interfaces:**
- Produces a `parse(text) -> list[Frame]` for each family, registered in `parse_frames`. Library frames:
  - Go: `runtime/`, `/usr/local/go/src/`, `vendor/`, `go/pkg/mod/`.
  - PHP: `/vendor/`.
  - Ruby: `/gems/` and `/ruby/<n>`.
  - Native: symbolized `#N 0x… in fn file:line` frames only.

- [ ] **Step 1: Write the failing tests:**
  - For `go-nil-map`, `php-division-by-zero`, `ruby-nomethod` and `cpp-out-of-range`, the first app frame matches `expected.json`.
  - `test_bare_segfault_yields_no_frames` (`c-segfault`).
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): Go, PHP, Ruby and native stack-trace parsers`.

### Task 27: Assess rules

**Files:**
- Create: `agents/incident_triage/assess.py`
- Test: `tests/test_assess.py`

**Interfaces:**
- Consumes: the evidence `meta` keys (Task 23) and `parse_frames` (Task 25)
- Produces:
  - `def assess(k8s: Sequence[EvidenceItem], frames: Sequence[Frame], *, has_logs: bool) -> Assessment`
  - `async def assess_node(state, runtime) -> dict`. It returns `{"assessment": Assessment}`.
  - `def route_after_assess(state: TriageState) -> Literal["collect_code", "analyze"]`
- The signal lists are exactly the ones in §9.5. The rule: `collect_code = bool(app_signals) or not infra_signals`. Every decision adds one human-readable entry to `reasons`.

- [ ] **Step 1: Write the failing test.** `test_assess_table` is parametrized over these rows:

| Evidence | `collect_code` |
|---|---|
| OOMKilled only | False |
| OOMKilled plus Java `OutOfMemoryError` frames | True |
| ImagePullBackOff | False |
| CreateContainerConfigError | False |
| Unhealthy probe events with no trace | False |
| Evicted | False |
| exit 1 with a Python trace | True |
| exit 139 with no trace | True |
| no signals at all | True |

  It asserts the signal names too. Also add `test_route_after_assess`.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS. Commit: `feat(triage): deterministic assessment rules`.

### Task 28: Source location, checkout and path mapping

**Files:**
- Create: `adapters/source/{locator.py,ecr.py,git.py}`, `codeintel/paths.py`
- Modify: `adapters/__init__.py`
- Test: `tests/test_locator.py`, `tests/test_git_source.py`, `tests/test_paths.py`

**Interfaces:**
- Produces:
  - `ChainLocator(*, annotation_keys: tuple[str, str], ecr: EcrLabelReader | None, config_map: Path | None) -> SourceLocator`. The order is: annotations, then OCI labels `org.opencontainers.image.source` and `.revision`, then the config map (a YAML file mapping `namespace/workload` to repo and ref).
  - `EcrLabelReader(client)`, with `async def labels(image: str, digest: str | None) -> dict[str, str]`. It calls `batch_get_image`, reads the config digest from the manifest, calls `get_download_url_for_layer`, fetches the config JSON with `urllib.request`, and returns `config.Labels`.
  - `GitSourceProvider(cache_dir: Path, token: SecretStr | None, max_cache_mb: int) -> SourceProvider`:
    - it runs `git init`, `git fetch --depth 1 origin <sha>` and `git checkout --detach FETCH_HEAD` with `asyncio.create_subprocess_exec`;
    - it sets `GIT_TERMINAL_PROMPT=0`, and passes the token through `-c http.extraHeader=Authorization: Bearer <token>`. The token is never part of the URL or the argv seen by `ps`; use `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_0`/`GIT_CONFIG_VALUE_0` environment variables;
    - the cache directory is `<cache>/<sha256(repo)[:16]>/<commit>`;
    - it evicts by least recent mtime when the cache is over budget.
  - `LocalSourceProvider` (Task 21) resolves `fixture://<app>`.
  - `FileIndex.build(root: Path) -> FileIndex`, and `FileIndex.resolve(frame: Frame) -> str | None`:
    - it picks the longest path-suffix match;
    - JVM frames match on the package path;
    - JS frames fall back from `dist/`→`src/` and from `.js`→`.ts`/`.tsx`;
    - ties go to the shortest path;
    - no match gives None.

- [ ] **Step 1: Write the failing tests:**
  - `test_locator_prefers_annotations`.
  - `test_locator_reads_oci_labels_from_ecr`: uses botocore `Stubber` for `batch_get_image` and `get_download_url_for_layer`, and patches `urlopen` to return the config JSON.
  - `test_locator_config_map_fallback`.
  - `test_locator_none_when_unknown`.
  - `test_git_checkout_by_sha_uses_cache`: a local bare repo with 2 commits, checked out at the first SHA, gives the first commit's content; the second call runs no git process.
  - `test_git_token_not_in_argv`.
  - `test_cache_eviction_respects_budget`.
  - `test_suffix_resolution`: these cases are parametrized.
    - a java-spring frame resolves to `src/main/java/com/example/orders/OrderController.java`;
    - `/app/orders/service.py` resolves to `orders/service.py`;
    - `/app/dist/orders/service.js` resolves to `src/orders/service.ts`;
    - an unknown frame gives None.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): locate, fetch and map source code`.

### Task 29: tree-sitter extraction

**Files:**
- Create: `codeintel/languages.py`, `codeintel/extract.py`
- Test: `tests/test_extract.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class LanguageSpec(grammar: str, extensions: tuple[str, ...], definition_types: frozenset[str])`
  - `LANGUAGES: Mapping[str, LanguageSpec]`, one for each of the 12 grammars in Global Constraints.
  - `def language_for(path: str) -> LanguageSpec | None`
  - `@dataclass(frozen=True) class ExtractedDef(path, start_line, end_line, symbol: str | None, language, text)`
  - `def enclosing_definition(root: Path, rel_path: str, line: int, *, max_lines: int = 200) -> ExtractedDef | None`. It returns the smallest definition node that contains the line. If there is none, it falls back to a window of ±20 lines. Longer output is truncated to `max_lines` centred on the line. Parsers are cached per grammar, using `tree_sitter_language_pack.get_parser`.

- [ ] **Step 1: Write the failing tests:**
  - `test_enclosing_definition_per_language`: parametrized over the 12 app fixtures; for the expected line, `symbol == expected.code.symbol` and the span covers `start_line..end_line`.
  - `test_window_fallback_top_level`.
  - `test_unknown_extension_none`.
  - `test_truncation_centred`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): tree-sitter enclosing-definition extraction`.

### Task 30: ripgrep search, bundling and the `collect_code` node

**Files:**
- Create: `codeintel/search.py`, `codeintel/bundle.py`, `nodes/code.py`
- Test: `tests/test_search.py`, `tests/test_bundle.py`, `tests/test_code_node.py`

**Interfaces:**
- Consumes: Tasks 25, 26, 28 and 29
- Produces:
  - `class RipgrepMissing(RuntimeError)`. Its message contains the text "install ripgrep".
  - `def literals_from_logs(log_text: str, *, max_literals: int = 10) -> list[str]`:
    - it takes candidate lines that contain `Exception`, `Error`, `panic:`, `error=`, `FATAL` or `processing` after a log level;
    - it removes runs of 2 or more digits, `0x…` hex, UUIDs, quoted strings and IPs;
    - it keeps the longest constant substring of at least 12 characters.
  - `async def search_literals(root: Path, literals: Sequence[str], *, max_matches: int = 50) -> list[SearchMatch]`. It runs `rg --json -F --max-count 5 -e <lit>…` and returns `SearchMatch(path, line, literal)`.
  - `def build_bundle(*, primary, raise_sites, others, budget_chars: int, repo: str, commit: str) -> list[CodeSnippet]`:
    - when two snippets overlap in the same path, it keeps the larger one;
    - order is primary, then raise sites, then others;
    - it stops at the budget, but always keeps at least one snippet when there are any;
    - IDs are `code:snippet:<n>`, and text passes `redact`.
  - `async def collect_code(state, runtime) -> dict`. It returns `{"code_evidence": [...], "notes": [...]}`:
    - the top 3 resolved app frames are primary, and the remaining frames are others;
    - the definitions around literal matches are raise sites;
    - frames that don't resolve produce `note:code:unmapped_frames`;
    - when there's no source, it produces `note:code:source_unavailable`.

- [ ] **Step 1: Write the failing tests:**
  - `test_literals_table`.
  - `test_search_finds_raise_site` (python fixture).
  - `test_ripgrep_missing` (with `PATH` empty).
  - `test_bundle_order_dedupe_budget`.
  - `test_collect_code_python_first_snippet_is_bug` (the path equals the expected path).
  - `test_c_segfault_uses_log_literals`.
  - `test_collect_code_without_source_continues` (Review Focus 4).
  - `test_unmapped_frames_are_recorded` (Review Focus 4).
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): ripgrep search, code bundle and collect_code`.

### Task 31: Prompts, Bedrock adapter and the `analyze` node

**Files:**
- Create: `prompts.py`, `adapters/llm/bedrock.py`, `nodes/analyze.py`, and `fixtures/recorded/_test/{invalid-json,bad-citation,refusal,max-tokens}/v1/attempt-{0,1}.json`
- Modify: `adapters/__init__.py`
- Test: `tests/test_prompts.py`, `tests/test_bedrock.py`, `tests/test_analyze.py`

**Interfaces:**
- Produces:
  - `SYSTEM_PROMPT: str`, fixed, with the rules from §9.7.
  - `PROMPT_VERSION = "v1"`.
  - `def render_user_message(state: TriageState) -> str`. Its sections are Target, Assessment, Kubernetes evidence, Datadog evidence, Code evidence and Notes. Each item is rendered as `[<id>] <title>\n<text>`.
  - `def analysis_schema() -> dict`, from `TriageAnalysis.model_json_schema()`, cut down to the JSON-schema subset Bedrock accepts.
  - `def build_request_body(req: AnalysisRequest, settings: Settings) -> dict`. It contains the system prompt with a cache breakpoint, one user message, `max_tokens`, `effort`, and the structured-output schema. It has no `temperature`, `top_p`, `top_k` or `tool_choice`.
  - `def make_bedrock_client(settings) -> Any`. It creates a boto3 `bedrock-runtime` client with `Config(read_timeout=300, retries={"mode": "standard", "max_attempts": 3})`.
  - `class BedrockLlm(LlmAdapter)`. Its `analyze` calls `invoke_model` in `asyncio.to_thread` and parses the text, `stop_reason` and `usage`.
  - `class AnalysisInvalid(ValueError)`, with an `errors: list[str]` field.
  - `def validate_analysis(text: str, state: TriageState) -> TriageAnalysis`. It fails on a JSON error, a schema error, an unknown evidence ID, or a code citation that matches no collected snippet.
  - `async def analyze(state, runtime) -> dict`. It returns `{"analysis", "refusal", "usage", "model_id"}` and is wrapped with `trace_llm("bedrock.analyze")`.
- Attempt rules for `analyze`:
  - If `stop_reason == "refusal"`, it returns `analysis=None` and `refusal=True`.
  - If the reply is invalid or `stop_reason == "max_tokens"`, it makes one more attempt, with `"Your previous reply was invalid: <errors>"` appended.
  - If that attempt is also invalid, it raises `AnalysisInvalid`. That error isn't transient.

- [ ] **Step 1: Verify the request shape.** Read the current AWS Bedrock documentation for Claude Opus 5.5 with `invoke_model`. Check the body fields for structured output, effort, the prompt-cache breakpoint and `anthropic_version`. Write the findings and the URLs into `docs/practices/P16.md`. If `invoke_model` doesn't support structured output for this model, use `converse` and record that.
- [ ] **Step 2: Write the failing tests**

```python
def test_body_has_no_sampling_or_forced_tools():          # P18
    body = build_request_body(req, settings); assert not {"temperature","top_p","top_k","tool_choice"} & body.keys()
def test_body_uses_settings(): ...                         # max_tokens 16000, effort "medium", schema present
def test_system_prompt_identical_across_incidents(): ...   # P23: two fixtures → equal system block
async def test_valid_recorded_reply(): ...                 # python-keyerror → category "application"
async def test_bad_citation_repaired(): ...                # _test/bad-citation: 2 calls, valid analysis
async def test_invalid_twice_raises(): ...                 # _test/invalid-json → AnalysisInvalid
async def test_refusal_marks_needs_human(): ...            # _test/refusal → refusal True, analysis None
async def test_max_tokens_stop_triggers_repair(): ...      # Review Focus 5
def test_throttling_left_to_botocore():                    # Review Focus 5
    assert make_bedrock_client(s).meta.config.retries == {"mode": "standard", "max_attempts": 3}
    assert is_transient(ClientError({"Error": {"Code": "ThrottlingException"}}, "InvokeModel")) is False
def test_bedrock_invoke_shape_with_stubber(): ...          # botocore Stubber asserts modelId and JSON body keys
```

- [ ] **Step 3:** Run them. Expected: FAIL.
- [ ] **Step 4: Implement**, and write the `_test` recorded replies.
- [ ] **Step 5:** Run them. Expected: PASS. Commit: `feat(triage): bedrock structured analysis with validated citations`.

### Task 32: Publish, sinks and the results table

**Files:**
- Create: `agents/incident_triage/migrations/0001_triage_results.sql`, `nodes/publish.py`, `adapters/sinks/sqs.py`
- Modify: `adapters/__init__.py`
- Test: `tests/test_publish.py` (integration)

**Interfaces:**
- Consumes: `run_once` (Task 10), `current_pool` (Task 7), `TriageContext.run` (Task 21)
- Produces:
  - The table `triage_results`, with the columns in §7.3 and `run_id` as primary key.
  - `async def publish(state, runtime) -> dict`. It returns `{"published": [sink names]}`:
    - it builds a `TriageResult`;
    - when `current_pool()` is set, it runs `insert … on conflict (run_id) do nothing`;
    - for each sink it calls `run_once(pool, key=f"publish:{run_id}:{sink.name}", action="publish", args={"run_id": str(run_id)}, fn=…)`;
    - with no pool (Studio), it only logs.
  - `SqsSink(client, queue_url)`, with `name = "sqs"`. It sends `MessageAttributes.idempotency_key`. On a `.fifo` queue it also sends `MessageDeduplicationId=key` and `MessageGroupId=<namespace>`.

- [ ] **Step 1: Write the failing tests:**
  - `test_publish_twice_one_row_one_send` (P13).
  - `test_publish_without_pool_logs_only`.
  - `test_sqs_sink_fifo_sets_dedup_id` (with `Stubber`).
  - `test_result_row_validates_as_triage_result`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `feat(triage): idempotent result publishing`.

### Task 33: Graph assembly, registration, `langgraph.json` and Studio

**Files:**
- Create: `agents/incident_triage/graph.py`, `agents/incident_triage/spec.py`, `langgraph.json`, and `fixtures/recorded/<each of 16 ids>/v1/attempt-0.json`
- Modify: `worker/registry.py` (add exactly one line, `incident_triage.spec.spec`), `Makefile` (`studio`)
- Test: `tests/test_graph.py`

**Interfaces:**
- Produces:
  - `def build_graph() -> StateGraph`. The nodes are `resolve_target`, `collect_k8s`, `collect_datadog`, `assess`, `collect_code`, `analyze` and `publish`. It uses `context_schema=TriageContext`, `set_node_defaults(retry_policy=default_retry_policy(), timeout=step_timeout(120))`, and these per-node timeouts:

    | Node | Timeout (s) |
    |---|---|
    | `resolve_target` | 30 |
    | `collect_k8s` | 60 |
    | `collect_datadog` | 60 |
    | `assess` | 10 |
    | `collect_code` | 180 |
    | `analyze` | 330 |
    | `publish` | 30 |

  - `error_handler`s on `collect_datadog` and `collect_code`. Each adds `note:datadog:unavailable` or `note:code:unavailable` and returns `Command(goto=<next node>)`.
  - `graph = build_graph().compile()`, at module level, for Studio.
  - `spec = AgentSpec(name="incident_triage", build_graph=build_graph, input_model=IncidentMessage, dedup_key=dedup_key, make_context=make_context, initial_state=lambda m: {"message": m}, serde_types=SERDE_TYPES, migrations_dir=<agent>/migrations, retention_sql="delete from triage_results where created_at < %(cutoff)s")`.
  - `langgraph.json`:

    ```json
    {"python_version": "3.14", "dependencies": ["."], "graphs": {"incident_triage": "./src/langgraph_postgres_starter/agents/incident_triage/graph.py:graph"}, "env": ".env"}
    ```

- [ ] **Step 1: Write the failing tests:**
  - `test_full_run_on_fixture`: parametrized over all 16 fixtures. It uses `InMemorySaver`, simulated adapters and the recorded LLM, and checks that the analysis is valid and the category equals the expected category.
  - `test_infra_fixture_skips_code`: `collect_code` never appears in `astream(stream_mode="updates")`.
  - `test_datadog_failure_continues_with_note`.
  - `test_registry_includes_incident_triage`.
  - `test_langgraph_json_graph_imports`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement**, and write the 16 recorded replies. Each reply must pass `validate_analysis` against its fixture's evidence.
- [ ] **Step 4:** Run them. Expected: PASS.
- [ ] **Step 5:** Run `make studio` (`uv run langgraph dev --no-browser`), then `curl -s localhost:2024/ok`. Expected: the response contains `ok`. Stop the server.
- [ ] **Step 6: Commit.** `feat(triage): assemble graph, register agent, studio config`

### Task 34: Simulated end-to-end with Compose, and the agent's crash and resume

**Files:**
- Create: `scripts/send_incident.py`, `tests/e2e/test_triage_e2e.py`
- Modify: `Makefile` (`incident LANG=<lang>` maps a language to a fixture ID; `incident FIXTURE=<id>`), `.github/workflows/ci.yml` (`e2e` runs this file too)

**Interfaces:**
- Produces: `uv run scripts/send_incident.py --fixture <id> [--queue-url URL] [--endpoint-url URL]`. It sets `fired_at` to now.

- [ ] **Step 1: Write the failing tests** (`@pytest.mark.e2e`)
  - `test_all_fixtures_produce_results`:
    1. Start a worker with simulated adapters and the recorded LLM.
    2. Send all 16 fixtures.
    3. Within 120 s there must be 16 rows in `triage_results`.
    4. Each row validates as a `TriageResult`, and each category matches its fixture.
  - `test_kill_9_during_datadog_resumes_at_datadog`:
    1. Start with `APP_FAKE_DATADOG_DELAY_S=5` and a call-log directory, then send `python-keyerror`.
    2. Send SIGKILL when `datadog.search_events:start` appears.
    3. Start a new worker and wait for `SUCCESS`.
    4. Check the call log:
       - each `k8s.*` method appears as many times as in one clean run;
       - `datadog.search_events:start` appears twice;
       - `sink.log.send` appears once.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement** the script and the Makefile changes.
- [ ] **Step 4:** Run them. Expected: PASS.
- [ ] **Step 5:** Run `docker compose up -d --build && make incident LANG=python && docker compose logs worker | grep triage.published`. Expected: one line.
- [ ] **Step 6: Commit.** `test(triage): simulated end-to-end and resume at the datadog step`

### Task 35: Evals

**Files:**
- Create: `agents/incident_triage/evals/{__init__.py,dataset.py,evaluators.py,test_evals.py}`, `scripts/sync_dataset.py`, `.github/workflows/evals.yml`
- Test: `agents/incident_triage/evals/test_evaluators.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class EvalCase(fixture_id, expected: dict)`, and `def load_cases() -> list[EvalCase]`. It loads 16 cases.
  - These evaluators, each returning `{"key", "score", "comment"}`:
    - `category_match(outputs, reference)`
    - `code_location_hit(outputs, reference)`: passes when a suspected path equals the expected path and the line ranges overlap
    - `citations_valid(outputs, evidence_ids)`
    - `judge_root_cause(outputs, reference, *, llm: LlmAdapter)`: a Bedrock structured 0/1 score; it is skipped unless `llm_mode == "bedrock"`
  - Thresholds:

    | Evaluator | Threshold |
    |---|---|
    | category | ≥ 0.9 |
    | code location (application cases) | ≥ 0.8 |
    | citations | 1.0 |

  - `test_evals.py` uses `@pytest.mark.langsmith` when `LANGSMITH_API_KEY` is set.
  - `evals.yml` runs on `pull_request` and `workflow_dispatch`:
    - it runs only when `github.event.pull_request.head.repo.full_name == github.repository`;
    - it gets AWS credentials through OIDC with pinned `aws-actions/configure-aws-credentials`;
    - it sets `APP_LLM_MODE=bedrock`;
    - it writes a scorecard to `$GITHUB_STEP_SUMMARY`.
  - Normal CI runs the deterministic evaluators in recorded mode.

- [ ] **Step 1: Write the failing tests:**
  - `test_load_cases_16`.
  - `test_category_match`.
  - `test_code_location_overlap`.
  - `test_citations_valid_rejects_unknown_id`.
  - `test_judge_skipped_in_recorded_mode`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them, then run `uv run pytest src/langgraph_postgres_starter/agents/incident_triage/evals -q`. Expected: PASS, with the thresholds met in recorded mode. Commit: `feat(triage): evals and secret-gated live eval workflow`.

### Task 36: Kubernetes manifests and IAM policy

**Files:**
- Create: `deploy/k8s/{kustomization.yaml,deployment.yaml,serviceaccount.yaml,rbac.yaml,configmap.yaml,migrate-job.yaml}`, `deploy/iam/policy.json`
- Test: `tests/unit/test_manifests.py`

**Interfaces:**
- Produces:
  - **Deployment:**
    - `terminationGracePeriodSeconds: 60`
    - liveness probe `/ok`, readiness probe `/ready`, port 8080
    - `POD_NAME` from `fieldRef: metadata.name`
    - `runAsNonRoot: true` and `readOnlyRootFilesystem: true`, with `emptyDir` volumes for `/tmp` and the source cache
  - **RBAC:** a ClusterRole with verbs `get` and `list` on `pods`, `pods/log`, `events`, `replicasets`, `deployments`, `statefulsets`, `daemonsets`, `jobs` and `nodes`.
  - **IAM policy actions:** `bedrock:InvokeModel`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `sqs:SendMessage`, `ecr:BatchGetImage`, `ecr:GetDownloadUrlForLayer`.

- [ ] **Step 1: Write the failing tests:**
  - `test_rbac_read_only` (P19): verbs ⊆ {get, list}, and the exact resource set.
  - `test_deployment_shutdown_and_probes`.
  - `test_security_context`.
  - `test_iam_actions_exact`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `deploy: example EKS manifests, read-only RBAC, IAM policy`.

### Task 37: Docs, practice notes, rename and remove scripts, and the removal job

**Files:**
- Create:
  - `docs/practices/P1.md … P23.md`, each at most 40 lines, with the sections Problem, What the template does, Where (file paths) and Proof (test path)
  - `docs/guides/{quickstart,run-on-eks,customize,upgrade}.md` (`run-on-eks.md` documents `python -m asyncio ps <pid>`)
  - `scripts/rename.py`, `scripts/remove_example.py`
- Modify: `README.md` (final, §13.7), `AGENTS.md` (final; how to add a collector, a language and an agent), `.github/workflows/ci.yml` (add the `removal` job)
- Test: `tests/unit/test_docs.py`, `tests/unit/test_scripts.py`

**Interfaces:**
- Produces:
  - `scripts/rename.py <new_package_name> [--dry-run]`
  - `scripts/remove_example.py`. It deletes `agents/incident_triage/`, the `graphs` entry in `langgraph.json`, and the registry line.
  - The `removal` job: copy the repo into a temp directory, run `remove_example.py`, then run `uv run pytest -m "not integration and not e2e"` and `! grep -r incident_triage src`.

- [ ] **Step 1: Write the failing tests:**
  - `test_every_practice_has_a_note`: P1–P23 exist, and the test path each note names exists.
  - `test_readme_quickstart_targets_exist`.
  - `test_rename_dry_run`: on a temp copy, no `langgraph_postgres_starter` string is left under `src/`.
  - `test_remove_example_in_temp_copy`: the leftover suite imports cleanly.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Write the docs and the scripts.**
- [ ] **Step 4:** Run them. Expected: PASS. Commit: `docs: practice notes, guides, rename/remove scripts, removal job`.

### Task 38: Real-mode guide and release checklist

**Files:**
- Create: `docs/guides/real-mode.md`, `docs/release-checklist.md`
- Modify: `CHANGELOG.md`
- Test: `tests/unit/test_real_mode_guide.py`

**Interfaces:**
- Produces `real-mode.md`, with these steps in order:
  1. Set up the IAM role through Pod Identity or IRSA, using `deploy/iam/policy.json`.
  2. Apply RBAC.
  3. Create the Secrets: `DD_API_KEY`, `DD_APP_KEY` and `APP_GIT_TOKEN`.
  4. Set `APP_ADAPTERS=real` and `APP_LLM_MODE=bedrock`.
  5. Run the migrate Job.
  6. Deploy.
  7. Send a real incident with `scripts/send_incident.py --message-file`.
  8. Check that the `TriageResult` looks right.

- [ ] **Step 1: Write the failing test.** `test_guide_lists_every_real_mode_setting`: every `Settings` field that real adapters read appears in the guide.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3: Write the guide and the checklist.** Then add an `0.1.0` entry to `CHANGELOG.md` and mark it "pending owner smoke test".
- [ ] **Step 4:** Run the full suite with `make lint typecheck test test-int test-e2e`. Expected: all pass. Commit: `docs: real-mode guide and release checklist`.
- [ ] **Step 5: Owner step.** Ask the owner to run the real-mode smoke test from the guide. Write the date and the result into `CHANGELOG.md`. Create the tag `v0.1.0` only after the owner confirms it.
