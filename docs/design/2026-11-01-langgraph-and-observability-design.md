# langgraph-and-observability: Design (v2)

- **Date:** 2026-11-01. Version 2 was written on 2026-11-02 after the owner changed the first agent.
- **Status:** The owner agreed and asked for the implementation plan on 2026-11-02 ("go ahead", "create a plan"). The owner has not reviewed every section. Items marked _(Designer's call)_ are decisions the owner can override.
- **Name:** `langgraph-and-observability`. The Python package is `langgraph_and_observability`, and `scripts/rename.py` renames it for forks.
- **Repository:** [github.com/3ohm/langGraph-and-observability](https://github.com/3ohm/langGraph-and-observability), public since 2026-11-05. On 2026-11-05 the owner renamed the project from its working name `langgraph-postgres-starter` to `langgraph-and-observability` (package `langgraph_and_observability`; the image and the Kubernetes names use the hyphenated form, because Kubernetes names must be lowercase). The name is temporary: the owner plans another rename when the Support Desk agent arrives. For that rename, run `scripts/rename.py`, then update the bracketed template names in `docs/guides/upgrade.md` and `TEMPLATE_PACKAGE` in `tests/unit/test_scripts.py`.

## 1. What this is and why

This is an open-source template repository for building LangGraph agents that hold up in production. It ships with Postgres persistence and LangSmith built in, and it targets Python 3.14. People fork it, delete the parts they don't need, and build their own agent on top. Every production practice in it is real code, has a test, and comes with a short note that explains why.

**The first agent is Incident Triage.** It's queue-driven and read-only. A message on Amazon SQS names a failing Kubernetes workload. The agent then works through these steps:

1. It collects evidence from Kubernetes: pod status, events, and logs, including the logs of the previous container.
2. It collects Datadog events for the same workload.
3. If that evidence doesn't explain the failure (for example, the app itself crashed), it collects code evidence. It parses the stack trace, searches the app's source code at the deployed commit with ripgrep, and extracts the enclosing functions with tree-sitter.
4. It sends the bundled evidence to Claude on AWS Bedrock (via boto3).
5. It stores the typed JSON result it gets back.

Every step is saved to Postgres. A run that dies at any step resumes at that step on another pod.

**Why it should exist** (research on 2026-11-01; sources are in Appendix B):

- LangGraph's docs only show beginner-level persistence. They use a single connection and cover no pooling, retention or crash recovery.
- The best-known community templates don't handle pool tuning, retention, concurrency, or evals in CI.
- LangGraph 1.2 added production features that no popular template uses yet: draining, step timeouts, error handlers and checkpoint pruning.
- In comparable repos, a working agent that does a real job draws 10–50× more interest than a blank skeleton.

**The owner's priorities:**

- Usefulness first, and virality second.
- Copying it and using it must be easy.
- Best practices are built in at the level of concepts and at the level of code.
- LangSmith is tightly integrated.
- The target is Python 3.14.

## 2. Who it's for

- **Platform and SRE teams** who want an automated first look at crashing workloads.
- **Python developers** who know the basics of LangGraph and need to ship a durable agent.
- **Coding assistants** that extend a fork. `AGENTS.md` tells them the repo's rules.

## 3. Goals (v1)

1. **Template, not framework.** The code is plain LangGraph and Python, with no wrapper layer.
2. **Every practice is backed by code, a test and a note** (§11).
3. **Incident Triage works fully in its real environment:** EKS, SQS, Kubernetes API, Datadog, Git and Bedrock.
4. **It's crash-safe.** After any interruption, a run resumes at the step that was interrupted. Earlier steps are never repeated.
5. **Postgres persistence is production-grade** (§7).
6. **LangSmith tracing and evals are built in.** Evals gate pull requests.
7. **It runs locally with no credentials.** Simulated adapters stand in for Kubernetes, Datadog and Git, and recorded model responses stand in for Bedrock. `docker compose up` shows the whole flow.
8. **Studio works.** `make studio` opens the graph in LangGraph Studio with simulated data.
9. **It's ready for coding assistants,** through `AGENTS.md`.
10. **It stays current:** CI on Python 3.14, and scheduled dependency updates.

## 4. Non-goals (v1) and backlog

**Non-goals for v1:**

- A pip-installable framework.
- Any action that changes the cluster. The agent is read-only and does no remediation.
- A chat interface, an Agent Server-compatible API, or end-user authentication.
- Watching several clusters. v1 triages workloads in the cluster where it runs.
- Helm charts, Terraform or autoscaling. The owner's team handles scaling. v1 ships minimal example manifests only (§10.1).
- Free-threaded Python (3.14t). psycopg-binary and orjson have no 3.14t wheels.
- DeltaChannel. It's still beta, and checkpoints written with it can't be read by older versions.
- Apps written in Rust or .NET (the owner's rule).

**Backlog, in no fixed order:**

- **Support Desk agent:** a customer-support example in which the customer confirms refunds and address changes. It needs the items below.
- **Chat infrastructure:**
  - an Agent Server-compatible HTTP API with SSE, compatible with agent-chat-ui;
  - end-user auth and per-user ownership of conversations;
  - human-in-the-loop approvals.
- **Long-term memory for the example agent,** for example past incidents for each workload. The LangGraph Store itself is no longer backlog: it runs on the shared pool (§7.3, P24).
- **LangSmith Deployment** as a hosting option for the graph.
- **Result sinks** for Slack, Jira and PagerDuty.
- **Producer adapters** that turn Datadog monitor webhooks and Alertmanager alerts into the input envelope.
- **Source-map support** for compiled JavaScript and TypeScript.
- **Multi-cluster** triage.
- **Phase 2 observability:** open-source backends via OpenTelemetry (§12.2).
- **End-to-end tests** against a kind cluster with real crashing apps.

## 5. Anti-goals: things that would count as failure even if it works

- A fork that takes a day to strip down (§13.9).
- Helpers that hide LangGraph's APIs.
- Vendor SDK imports scattered through agent code. Vendors are reached only through adapters.
- A quickstart that needs credentials.
- A health check that stays green while the database is broken, or default secrets anywhere.
- A step that repeats already finished work after a crash.
- Sending secrets found in logs or code to the LLM, or to traces.
- A result that cites evidence the agent never collected.

## 6. Constraints and verified facts (2026-11-01/02)

### 6.1 Python

- **Target:** 3.14, the owner's decision. The project sets `requires-python = ">=3.14"` and `.python-version` is `3.14`.
- **Verified on CPython 3.14.3:**
  - langgraph 1.2.19 runs a checkpointed graph;
  - tree-sitter 0.26.0 with tree-sitter-language-pack 1.23.1 parses all twelve target grammars;
  - datadog-api-client 2.62.0 imports (it's pure Python; its PyPI classifiers only go up to 3.12);
  - kubernetes-asyncio 36.1.0 and boto3 1.43.123 import.
- **LangGraph's own CI** tests 3.10–3.14.
- **Python 3.15** came out on 2026-10-09. On 2026-11-05 the owner decided: Python 3.14 only, and no 3.15 CI job.
- **Warning filter:** on 3.14, langsmith emits `DeprecationWarning: 'asyncio.iscoroutinefunction' is deprecated`. Filter that warning by name in the pytest config.
- **3.14 features to use:**
  - `uuid.uuid7()` for time-ordered IDs;
  - deferred annotations (no `from __future__ import annotations`);
  - `python -m asyncio ps <pid>`, documented for debugging stuck runs.

### 6.2 Versions at design time

These are minimum versions; the lockfile pins exact versions.

| Package | Version |
|---|---|
| langgraph | 1.2.19 (`>=1.2.19,<2`) |
| langgraph-checkpoint / langgraph-checkpoint-postgres | 4.3.1 / 3.2.0 (first with `prune`) |
| langgraph-cli | 0.4.40 (for `langgraph dev`) |
| langchain-core | 1.7.3 |
| langsmith | 0.14.16 |
| psycopg / psycopg-pool | 3.3.6 / 3.3.3 |
| pydantic / pydantic-settings | 2.14.2 / 2.16.2 |
| fastapi / uvicorn | 0.162.0 / 0.54.0 (health endpoints only) |
| kubernetes-asyncio | 36.1.0 |
| datadog-api-client | 2.62.0 |
| boto3 | 1.43.123 (owner's choice for Bedrock; also SQS and ECR) |
| tree-sitter / tree-sitter-language-pack | 0.26.0 / 1.23.1 |
| structlog | 26.1.0 |
| pytest / pytest-asyncio / testcontainers | 9.2.0 / 1.4.1 / 4.15.0 |
| ruff / mypy | 0.17.3 / 2.4.1 |
| ripgrep (binary in the image) | 15.1.0 locally; Debian trixie package in the image |
| git (binary in the image) | Debian trixie package |

### 6.3 Infrastructure

- **Postgres 18**, using the image `pgvector/pgvector:0.8.4-pg18`. pgvector isn't used in v1, but with this image the Store (P24) can add semantic search without a different database.
- **Local SQS:** ElasticMQ (`softwaremill/elasticmq-native:1.7.1`, Apache-2.0), which is SQS-compatible.
- **Runtime image:** `python:3.14-slim` (Debian trixie), built with uv, with `ripgrep` and `git` installed from apt.
- **Docker:** no daemon was running on this machine on 2026-11-02. Compose and the integration tests need Docker Desktop, OrbStack or colima.

### 6.4 Claude on Bedrock (verified against the Claude API reference, 2026-11-02)

- **Default model:** Claude Opus 5.5, the owner's decision. The Bedrock model ID is `anthropic.claude-opus-5-5`. Some accounts need an inference-profile prefix such as `us.` or `global.`; the builder verifies this in the owner's account. Prices are $4 per million input tokens and $20 per million output tokens.
- **Structured outputs** are supported (GA) on Bedrock.
- **Forced tool choice** (`any`, or a named tool) returns a 400 on Opus 5.5. Don't use the old "force a tool to get JSON" trick.
- **Sampling parameters:** sending `temperature`, `top_p` or `top_k` returns a 400. Never set them, including for the LLM judge.
- **Assistant prefill** returns a 400.
- **Thinking** can't be turned off on Opus 5.5. Effort is the control. It defaults to `medium`, and the agent sets it explicitly.
- **Refusals:** a response can stop with `stop_reason: "refusal"`. The agent records this and marks the result `needs_human`; it never crashes.
- **`max_tokens`:** default to about 16,000 for a non-streaming call.
- **Prompt caching** is supported on Bedrock. It matches on a prefix, so keep the system prompt byte-for-byte stable.
- **boto3:** the owner chose it. The builder verifies the exact request shape for structured output (`invoke_model` with the Anthropic request body, or `converse`) against current AWS documentation before writing that code. The Claude API reference also describes Anthropic's own Bedrock client, which uses the same AWS auth and validates typed output; the owner kept boto3.

### 6.5 LangGraph's default retry rule (verified in source)

`default_retry_on` does three things that don't suit this agent:

- It retries raw `httpx` errors only on 5xx, so a raw 429 isn't retried.
- It never retries `TimeoutError`, because that's an `OSError`.
- It *does* retry provider-SDK errors it doesn't recognise, including 400s.

**Decision:** external calls get one retry layer, the client library's own (botocore's standard retry mode, and the Datadog and Kubernetes clients' retries). The graph's `RetryPolicy` uses an explicit `retry_on` that retries only transient infrastructure errors: connection errors, timeouts, and Postgres serialization or deadlock errors. It never retries a 4xx.

## 7. Persistence (Postgres)

### 7.1 Connections

- **One `psycopg_pool.AsyncConnectionPool` per process,** opened at startup with `wait=True`. Startup fails if Postgres can't be reached.
- **Pool settings:**
  - `autocommit=True`, `row_factory=dict_row`;
  - `check=AsyncConnectionPool.check_connection`;
  - `max_lifetime` below the idle timeout of any proxy in front of the database;
  - TCP keepalives in the connection string;
  - pool sizes from settings.
- **PgBouncer transaction mode** (`APP_PGBOUNCER_MODE=true`) sets `prepare_threshold=None` and turns off the saver's pipeline mode. This follows upstream issue #8420; the builder verifies the exact switch.
- **Two database roles:** a migration role that runs DDL, and an app role limited to DML.

### 7.2 Checkpointer

- `AsyncPostgresSaver` runs on the shared pool. Each triage run is one LangGraph thread, and `thread_id` equals `run_id`.
- **Known bottleneck** (verified in source, 3.2.0): `_cursor()` takes a lock across the whole instance even when the saver is backed by a pool, so checkpoint I/O runs one call at a time. The fix is a small pool-aware subclass that mirrors `AsyncPostgresStore`. It comes with:
  - a test that fails if the private upstream method changes shape;
  - a concurrency test;
  - an upstream issue and pull request.

  Delete the subclass once upstream fixes this.
- **Serialization hardening:**
  - `LANGGRAPH_STRICT_MSGPACK=true`;
  - an explicit allowlist of our state types;
  - `pickle_fallback=False`;
  - a save-and-reload test for every state type.
  - Optional encryption at rest with `EncryptedSerializer.from_pycryptodome_aes()` when `LANGGRAPH_AES_KEY` is set.

  Context: CVE-2025-64439 and CVE-2026-27794.

### 7.3 Tables and migrations

Migrations are plain SQL, forward-only and numbered, with one folder per component. The runner applies core first, then the worker, then each registered agent. It records `(component, version)` in `schema_migrations`.

| Component | Folder | Tables |
|---|---|---|
| core | `persistence/migrations/` | `action_ledger` (key PK, action, args hash, result, created_at) |
| worker | `worker/migrations/` | `runs` (see below) |
| incident_triage | `agents/incident_triage/migrations/` | `triage_results` (see below) |

**`runs` columns:**

- `run_id`: uuid7 primary key, which is also the LangGraph `thread_id`;
- `agent`;
- `dedup_key`: unique;
- `status`: pending, running, success, error or failed;
- `input`: jsonb, the validated envelope;
- `attempt`;
- `lease_owner`: the pod name;
- `lease_expires_at`;
- `created_at`, `started_at`, `ended_at`;
- `error`: jsonb.

There's an index on `(status, lease_expires_at)`.

**`triage_results` columns:**

- `run_id`: primary key;
- `dedup_key`;
- `target`: jsonb;
- `category`;
- `result`: jsonb, the validated `TriageResult`;
- `model_id`;
- `usage`: jsonb;
- `created_at`.

**How migrations run:**

- **The `migrate` command** runs our SQL migrations, then `AsyncPostgresSaver.setup()` and `AsyncPostgresStore.setup()`: the checkpoint tables and the tables of the LangGraph Store, which the worker compiles every graph with (P24).
- **It's a one-shot step** under the migration role, run as a Compose service or a Kubernetes Job. It never runs during startup on every replica.
- **Autocommit:** both `setup()` calls need it, because they use `CREATE INDEX CONCURRENTLY`.

### 7.4 Retention

A pruner runs inside the worker. It holds a Postgres advisory lock, so only one pod prunes at a time. It does two jobs:

- **Old checkpoints.** For runs that finished more than `APP_RETENTION_CHECKPOINT_DAYS` ago (default 14), it calls `prune([run_ids], strategy="delete")`. The results stay in `triage_results`.
- **Old results.** It deletes `triage_results` and `runs` rows older than `APP_RETENTION_RESULT_DAYS` (default 90).

It retries on `SerializationFailure` and `DeadlockDetected`. It never deletes from LangGraph's tables with raw SQL.

## 8. Runtime: the queue worker

### 8.1 Intake (SQS)

1. Long-poll SQS (`WaitTimeSeconds=20`), asking only for as many messages as there are free slots.
2. Validate each message against the `IncidentMessage` envelope (§9.1). If it's invalid, log it and **don't delete it**. SQS moves it to the dead-letter queue after `maxReceiveCount`.
3. Compute the dedup key (§9.11). Run `INSERT INTO runs ... ON CONFLICT (dedup_key) DO NOTHING`.
4. Delete the message. From this point Postgres, not SQS, owns the run. The visibility timeout only has to cover steps 2–4.
5. If the insert created a new run, schedule it. If a run already existed for that key, log the message as a duplicate.

### 8.2 Execution

- **Concurrency:** each run executes as an asyncio task, with at most `APP_WORKER_CONCURRENCY` runs per pod (default 4).
- **Claiming a run:** the pod sets `lease_owner` to its name and `lease_expires_at` to now plus `APP_LEASE_SECONDS` (60), and sets `status=running`.
- **Heartbeat:** every `APP_HEARTBEAT_SECONDS` (15), the pod extends the leases of its own runs.
- **Invocation:** the pod invokes the graph with `thread_id=run_id`, `durability="sync"`, a fresh `RunControl`, and a run deadline of `APP_RUN_DEADLINE_SECONDS` (default 900).
- **Outcomes:**
  - On success, the run is marked `success`.
  - On an error after the graph's own retries, the attempt count goes up and the run returns to `pending` for recovery.
  - After `APP_MAX_ATTEMPTS` attempts (default 3), the run is marked `failed`, with the error recorded.
  - When the graph stops at an interrupt, the attempt fails as on an error (`InterruptNotSupportedError`): the worker has no resume value. The run gets a retry, and LangGraph runs only the interrupted node again; the steps that completed do not run again. An interrupt is never a `success` (owner decision 2026-11-05).

### 8.3 Recovery (the periodic check)

- **Every `APP_RECOVERY_INTERVAL_SECONDS`** (default 30), and once at startup, each pod claims runs that meet both conditions:
  - status is `pending`, or status is `running` with an expired lease;
  - the claim uses `FOR UPDATE SKIP LOCKED`, so two pods never take the same run.
- **Resuming:** the pod calls `invoke(None, config)`, which continues from the last saved step. A run with no checkpoint yet starts from its saved input.
- **Why it's needed:** this is what recovers runs after OOMKill, SIGKILL or node loss, when no shutdown hook runs.

### 8.4 Planned shutdown (SIGTERM, with the owner's 60-second hook)

1. Stop polling SQS.
2. Call `request_drain()` on every active run's `RunControl`. Each run stops after its current step, at a checkpoint it can resume from (`GraphDrained`).
3. Wait at most `APP_DRAIN_TIMEOUT_SECONDS` (default 45, below the 60-second grace period).
4. Set drained runs to `pending` and clear their lease, so another pod's periodic check resumes them.
5. Close the pool and flush traces.

If a step lasts longer than the grace period, Kubernetes sends SIGKILL, and that step starts again from its beginning. This is safe, because every step only reads data, except `publish`, which is idempotent (§9.9).

### 8.5 Correctness rules

- **Defaults:** set graph-wide with `set_node_defaults(retry_policy=..., timeout=TimeoutPolicy(...), error_handler=...)`, using the retry rule in §6.5. Every step is async, because node timeouts only work on async nodes.
- **What goes where:** `context_schema` holds the run's fixed inputs (run ID, target, settings handles). `configurable` carries only `thread_id`, because langchain-core copies primitive `configurable` values into trace metadata. Secrets never go in either; adapters read them from settings.
- **Size:** state holds evidence as typed, size-capped lists. Large blobs never go into state.

### 8.6 Health and inspection

- **Health:** a small FastAPI app on `APP_HEALTH_PORT` serves two endpoints.
  - `/ok` is liveness: the event loop responds.
  - `/ready` is readiness: Postgres answers `SELECT 1`, the pool is open, and the intake loop is alive.
- **Inspection:** a CLI, `python -m langgraph_and_observability runs list|show <run_id>|retry <run_id>`.

## 9. The first agent: Incident Triage

### 9.1 Input envelope (`IncidentMessage`, Pydantic)

_(Designer's call.)_

| Field | Type | Notes |
|---|---|---|
| `version` | `Literal[1]` | |
| `source` | `str` | For example `datadog-monitor`, `alertmanager` or `manual` |
| `alert_id` | `str \| None` | Used in the dedup key when present |
| `fired_at` | `datetime \| None` | Defaults to the time the message was received |
| `cluster` | `str \| None` | If set, it must match `APP_CLUSTER_NAME` |
| `namespace` | `str` | Required |
| `pod` | `str \| None` | One of `pod` or `workload` is required |
| `workload` | `{kind: "Deployment" \| "StatefulSet" \| "DaemonSet" \| "Job", name: str} \| None` | |
| `container` | `str \| None` | |
| `raw` | `dict \| None` | The producer's original payload, kept for reference |

### 9.2 Graph

`resolve_target` → `collect_k8s` → `collect_datadog` → `assess` → (`collect_code` if needed) → `analyze` → `publish` → END.

- Each step is an async node with its own timeout.
- Evidence items get stable IDs, such as `k8s:event:3`, `log:previous:120`, `dd:event:<id>` and `code:snippet:2`. Every item and every list has a size cap.
- `assess` routes either to `collect_code` or straight to `analyze`.

### 9.3 Kubernetes evidence (kubernetes-asyncio)

- **Config:** in-cluster config in EKS, kubeconfig in dev.
- **`resolve_target`:** maps a pod to its owner workload, or a workload to its current failing pod. It records the containers, images and image digests.
- **`collect_k8s` collects:**
  - pod status;
  - container statuses: state, `lastState.terminated` (reason, exit code, signal) and restart count;
  - events for the pod and its owner;
  - logs for the current container and for `previous=True`, limited to the last `APP_LOG_TAIL_LINES` lines (default 500) and a time window;
  - the node's conditions.
- **Access:** read-only RBAC. A ClusterRole grants `get`/`list` on pods, pods/log, events, replicasets, deployments, statefulsets, daemonsets, jobs and nodes.

### 9.4 Datadog evidence (datadog-api-client, async)

- **Query:** `EventsApi.search_events`, filtered by tags (`kube_namespace`, `pod_name`, `kube_deployment` or similar).
- **Time window:** `[fired_at - APP_DD_LOOKBACK_MINUTES (30), fired_at + 5 minutes]`.
- **Limit:** at most `APP_DD_MAX_EVENTS` events (default 50).
- **Credentials:** `DD_SITE`, `DD_API_KEY` and `DD_APP_KEY`, from a Secret.

### 9.5 Assess: deterministic rules

_(Designer's call.)_ The rules live in one module and have table-driven tests.

- **Infrastructure or configuration signals:**
  - `OOMKilled`;
  - `ImagePullBackOff`, `ErrImagePull` or `InvalidImageName`;
  - `CreateContainerConfigError` or `CreateContainerError`;
  - the events `FailedScheduling`, `FailedMount`, `FailedAttachVolume` or `Evicted`;
  - probe failures (`Unhealthy`);
  - node `NotReady` or pressure conditions.
- **Application signals:**
  - a stack trace, panic or uncaught exception parsed from the logs;
  - a non-zero exit (for example 1, 2, 134 or 139) with no infrastructure signal.
- **Decision:** skip code evidence only when there are infrastructure signals and no application signals. Otherwise collect code evidence. The decision and its reasons are stored in state and in the result.

### 9.6 Code evidence

The agent supports these languages (the owner's list): JavaScript/TypeScript (Node.js), Python, Java (Spring Boot and plain Java), Go, PHP, Ruby, Kotlin, Scala, C and C++.

**1. Finding the source** _(Designer's call)_. The agent tries these sources in order:

- workload annotations, with the keys for repository URL and commit set in settings;
- OCI image labels `org.opencontainers.image.source` and `.revision`, read from ECR with boto3 (`ecr:BatchGetImage` plus the image config blob);
- a config file (`APP_SOURCE_MAP_FILE`) that maps each workload to a repository and a ref.

It then fetches that commit into a cache with the `git` CLI (`git init`, `git fetch --depth 1 origin <sha>`, `git checkout`). The HTTPS token comes from settings. The cache, `APP_SOURCE_CACHE_DIR`, is keyed by repository and SHA and has a size-bounded LRU clean-up. Tests use a local-folder adapter instead.

**2. Stack traces.** There is one parser per family:

- JVM (Java, Kotlin, Scala);
- Node.js;
- Python;
- Go (panics and goroutine traces);
- PHP;
- Ruby;
- native C/C++ (symbolized backtraces and sanitizer output).

Each parser yields frames as `(file, line, function, is_app_frame)`. Library frames are filtered out per family, for example `site-packages`, `node_modules`, `java.*`, the Go runtime and `vendor/`. Native crashes often print no trace; in that case the agent searches for the log messages only.

**3. Mapping paths.** Each frame file is matched to the repository's file index by longest path suffix. JVM frames map package paths to file paths, and JS frames go from `dist/` to `src/` (`.js` → `.ts`). Frames that can't be matched are recorded.

**4. Extraction (tree-sitter).** For each matched frame, the agent extracts the smallest definition node that encloses the line: a function, method or class. Each language has a table of definition node types, with a test per language. Each snippet records the repository, commit, path, start and end lines, and symbol.

**5. Search (ripgrep).** The agent runs `rg --json -F` for exception messages and for log-message constants, with variable parts removed (numbers, hex, UUIDs, quoted values). Matches are capped. The definitions that enclose them are then extracted.

**6. Bundling.** Overlapping snippets are removed. The rest are ranked: the top application frame first, then the places that raise or log the error, then the other application frames. The bundle is held within `APP_CODE_BUDGET_CHARS` (default 60,000).

### 9.7 Analyze (Bedrock via boto3)

- **Prompt:**
  - a fixed system prompt that defines the role and three rules:
    - cite only evidence IDs from the provided list;
    - answer `unknown` when the evidence isn't enough;
    - never invent code locations;
  - a user message with the evidence, in sections, each item tagged with its ID.

  The system prompt carries a cache breakpoint.
- **Structured output:** the JSON schema comes from the Pydantic `TriageAnalysis` model and is sent through Bedrock's structured-output support (§6.4). The response is validated with Pydantic in three checks:
  - every cited evidence ID exists;
  - every code reference matches a collected snippet;
  - lists stay within their caps.
- **Validation failure:** one repair retry, with the validation errors appended. If that fails too, the step fails and the retry and error-handler rules apply.
- **Model call settings:**
  - `APP_BEDROCK_MODEL_ID` (default `anthropic.claude-opus-5-5`);
  - `APP_AWS_REGION`;
  - `APP_MODEL_EFFORT` (default `medium`);
  - `APP_MAX_OUTPUT_TOKENS` (default 16000);
  - botocore `Config(read_timeout=300, retries={"mode": "standard", "max_attempts": 3})`;
  - no sampling parameters.
- **Async:** boto3 calls run in `asyncio.to_thread`.
- **Refusal:** the result gets `category=unknown`, `needs_human=true` and `refusal=true`.
- **Recorded mode:** with `APP_LLM_MODE=recorded`, the model call returns fixture responses keyed by incident fixture ID and prompt version. Tests and the credential-free demo use this; `bedrock` is the real mode.
- **Tracing:** the call is wrapped in `langsmith.traceable(run_type="llm")`. It records the model ID and token usage (input, output and cache reads) in metadata.

### 9.8 Result types (Pydantic)

_(Designer's draft.)_

**`TriageAnalysis`** is the LLM's output schema:

| Field | Type | Limit |
|---|---|---|
| `category` | `"infrastructure" \| "configuration" \| "application" \| "dependency" \| "unknown"` | |
| `summary` | `str` | 600 characters or fewer |
| `root_cause` | `str` | |
| `confidence` | `"low" \| "medium" \| "high"` | |
| `evidence` | `[{id, why}]` | At least 1, unless the category is `unknown` |
| `suspected_code` | `[{snippet_id, path, start_line, end_line, symbol}]` | |
| `recommended_actions` | `[str]` | 5 or fewer |
| `needs_human` | `bool` | |

**`TriageResult`** is what gets stored:

- `run_id`, `dedup_key` and `target`;
- `assessment` (the decision and its reasons);
- `analysis: TriageAnalysis | None`;
- `refusal`;
- `model_id` and `usage`;
- `evidence_counts`;
- `created_at`.

### 9.9 Publish

1. Insert into `triage_results`, idempotent on `run_id`.
2. Send the result to the configured sinks. Each send is idempotent through `action_ledger` with the key `publish:<run_id>:<sink>`. The v1 sinks are `log` (the default; one structured JSON log line) and `sqs` (an output queue set by `APP_RESULT_QUEUE_URL`). Slack, Jira and PagerDuty are in the backlog.

_(Designer's call. The owner hasn't said where results go yet; these sinks make adding a destination a small task.)_

### 9.10 Security and data handling

- **Read-only Kubernetes RBAC** (§9.3).
- **IAM** through EKS Pod Identity or IRSA, limited to:
  - `bedrock:InvokeModel`;
  - `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, and `sqs:SendMessage` (results only);
  - `ecr:BatchGetImage` and `ecr:GetDownloadUrlForLayer`.
- **Secrets** (the Datadog keys and the Git token) come in as environment variables from Kubernetes Secrets. Nothing has a default secret.
- **Redaction:** one module redacts secrets (tokens, keys, passwords, connection strings), emails and card numbers from logs, events and code before the evidence goes to Bedrock. The same rules apply to traces (§12.1).
- **Size caps** apply to every evidence type.

### 9.11 Dedup and alert storms

The dedup key is `sha256(cluster | namespace | workload-or-pod | alert_id or the 15-minute bucket of fired_at)`. Repeated alerts for the same workload in the same window map to one run.

## 10. Where it runs

### 10.1 EKS (the owner's environment)

- **The worker is a Deployment**, which the owner scales. It's configured with:
  - `terminationGracePeriodSeconds: 60`;
  - liveness and readiness probes on `/ok` and `/ready`;
  - `POD_NAME` from the downward API, used as the lease owner.
- **Example manifests** in `deploy/k8s/` (kustomize) _(Designer's call)_:
  - the Deployment;
  - a ServiceAccount, with the Pod Identity / IRSA annotation as a placeholder;
  - a read-only ClusterRole and its binding;
  - a ConfigMap example;
  - a `migrate` Job.

  There's no HPA, KEDA, Helm or Terraform.

### 10.2 Local (no credentials)

1. `docker compose up` starts `postgres`, `elasticmq` (with a DLQ redrive policy), `migrate` (which runs once) and `worker`. The worker runs in simulated mode: fake Kubernetes and Datadog adapters with fixtures, a local-folder source adapter, and the recorded LLM.
2. `make incident LANG=python` (or any other fixture) sends a sample message.
3. The result appears in the log and in Postgres.

Real mode is chosen per adapter in settings: `APP_ADAPTERS=simulated|real` and `APP_LLM_MODE=recorded|bedrock`.

### 10.3 Studio

`make studio` runs `langgraph dev`. `langgraph.json` registers the `incident_triage` graph, compiled without a checkpointer so that Studio supplies its own. It uses simulated adapters by default, and any fixture incident can be the input.

### 10.4 LangSmith Deployment

In the backlog.

## 11. Practices list (v1)

Each row ships as code, a test, and a short note in `docs/practices/`. The core practices (P8–P13) are proven on a small test-only graph in `tests/graphs/`, so deleting the example agent doesn't delete the proof. The agent also has its own tests.

| # | Practice | Proven by |
|---|---|---|
| P1 | Pooled, health-checked connections; startup fails without a database | Test that kills a connection; test that startup fails with no database |
| P2 | PgBouncer-safe mode | Test with `prepare_threshold=None` |
| P3 | Pool-aware checkpointer | Concurrency test, plus a test that catches upstream shape changes |
| P4 | Migrations as a separate step; minimal database roles | Migration smoke test in CI |
| P5 | Readiness that checks the database | Test: database down means not ready |
| P6 | Retention for checkpoints and results, run by one leader | Test with old runs |
| P7 | Strict serialization with an allowlist, optional encryption | Save-and-reload tests; an unknown type is rejected |
| P8 | Queue intake: validate, dedup, record, then delete; bad messages go to the DLQ | Tests against ElasticMQ: duplicate, invalid, redrive |
| P9 | Leases, heartbeats, periodic recovery | Test: a run with an expired lease is resumed by another worker |
| P10 | Drain within the 60-second hook | Test: SIGTERM stops after the current step; the run is resumed |
| P11 | Resume at the failed step | `kill -9` test during step 2; the restart resumes at step 2, and the fake adapter's call counts show step 1 isn't repeated |
| P12 | Explicit retry, timeout and error-handler defaults (§6.5) | Tests with failing and slow fakes: retried, not retried, timeout |
| P13 | Idempotent publishing | Crash after publish and before the next checkpoint: published once |
| P14 | No secrets or identity in `configurable`; a `context_schema` | Test on trace metadata |
| P15 | Redaction before the LLM and in traces; trace content modes | Tests per mode, plus a test with a secret in the logs |
| P16 | Typed structured output with validated citations and one repair retry | Tests with bad recorded responses |
| P17 | Bounded evidence (caps and the code budget) | Unit tests |
| P18 | Provider constraints: no sampling parameters, no forced tool choice, refusals handled | Unit tests on the request body and on a refusal fixture |
| P19 | Read-only, least-privilege access | Manifest test: RBAC verbs are only `get`/`list` |
| P20 | Deterministic, table-tested assessment rules | Table tests |
| P21 | Evals gate pull requests | CI job |
| P22 | Removable example agent | CI job deletes it, and the suite passes |
| P23 | Cache-friendly prompts | Test: the system prompt is identical across incidents |
| P24 | Long-term memory in the Postgres Store, on the shared pool | Test: a second thread reads what the first one wrote; tests in PgBouncer mode and with `migrate` twice |

## 12. Observability and evals

### 12.1 LangSmith

- **All vendor code lives in `observability/`.** Tracing is off unless `LANGSMITH_TRACING=true` and a key are set.
- **One `langsmith.Client`** is created at startup and flushed on shutdown, with:
  - `anonymizer`, using the redaction rules from §9.10;
  - `hide_inputs` / `hide_outputs`;
  - `tracing_sampling_rate`;
  - `tracing_error_callback`.
- **Content modes:** `APP_TRACE_CONTENT=full|redacted|none`. `redacted` is the default, `full` is allowed only in `dev`, and `none` keeps metadata only.
- **Metadata on every run:**
  - run ID and dedup key;
  - cluster, namespace and workload;
  - assessment category;
  - model ID;
  - environment, app version and git SHA.
- **Who owns which key:** the core's `run_metadata` gives the run ID (also as `session.id`, §12.2), dedup key, agent, environment, app version and git SHA. An agent adds its own keys with `AgentSpec.trace_metadata`, and a core key wins; the example adds cluster, namespace and workload.
- **Bedrock calls** are traced with `traceable(run_type="llm")`, with token usage.
- **Projects:** one per environment, `LANGSMITH_PROJECT=<name>-<env>`.
- **Logs** are structlog JSON and carry run and trace IDs.

### 12.2 Phase 2: open-source backends

OpenTelemetry is the common pipe. This covers LangSmith's OTel bridge, Phoenix (Elastic License 2.0; one container), Langfuse (MIT outside `ee/`; a heavy self-host) and Opik (Apache-2.0), plus a Collector that fans out to them. Phase 1 must not block this: trace fields map to standard names, for example `session.id` = run ID.

### 12.3 Evals

**Dataset:** `agents/incident_triage/evals/` contains one fixture incident per sample app (§13.10) plus infrastructure cases (OOMKilled, ImagePullBackOff, probe failure, CreateContainerConfigError). Each case records:

- the expected category;
- the expected code location (path and line range), for application cases;
- the key facts the answer must contain.

**Evaluators** are plain functions:

- category match;
- code location hit;
- citation validity;
- an LLM judge for the root-cause text, using Bedrock with `APP_JUDGE_MODEL_ID`, which defaults to the main model.

**CI:**

- Normal CI runs the deterministic evaluators in recorded mode.
- A secret-gated job runs everything against real Bedrock. It skips on forks and writes a scorecard to the job summary, linked to the LangSmith experiment.
- It fails if the score drops below a threshold.

## 13. Repository scaffolding (Phase 0)

### 13.1 Layout

```
.
├── .claude/agents/           implementer.md (Opus 5.5), reviewer.md (Fable 5.1)   [owner's build setup]
├── .github/                  workflows/ (ci.yml, evals.yml), ISSUE_TEMPLATE/, PULL_REQUEST_TEMPLATE.md, dependabot.yml
├── deploy/k8s/               kustomization.yaml deployment.yaml serviceaccount.yaml rbac.yaml configmap.yaml migrate-job.yaml
├── docs/
│   ├── design/               this document
│   ├── plans/                implementation plans
│   ├── adr/                  0001-template-not-framework.md, ...
│   ├── practices/            one note per practice (P1–P24)
│   └── guides/               quickstart, run-on-eks, real-mode, customize, upgrade
├── scripts/                  rename.py, remove_example.py, send_incident.py
├── src/langgraph_and_observability/
│   ├── __main__.py  settings.py  logging.py  ids.py  redaction.py
│   ├── persistence/          pool.py checkpointer.py serde.py ledger.py migrate.py  migrations/
│   ├── worker/               main.py intake.py runs.py recovery.py drain.py health.py cli.py registry.py  migrations/
│   ├── observability/        langsmith.py metadata.py
│   └── agents/incident_triage/
│       ├── graph.py state.py context.py schemas.py prompts.py assess.py
│       ├── nodes/            resolve.py k8s.py datadog.py code.py analyze.py publish.py
│       ├── adapters/         k8s/ datadog/ source/ llm/ sinks/   (each: interface + real + fake)
│       ├── codeintel/        stacktrace/ (jvm.py node.py python.py go.py php.py ruby.py native.py)
│       │                     paths.py extract.py search.py bundle.py languages.py
│       ├── fixtures/         incidents/  apps/<lang>/  recorded/
│       ├── migrations/  evals/  tests/
├── tests/                    unit/ integration/ e2e/ graphs/ fakes.py conftest.py
├── .env.example  .editorconfig  .gitignore  .dockerignore  .pre-commit-config.yaml  .python-version
├── AGENTS.md  CLAUDE.md  CHANGELOG.md  CODE_OF_CONDUCT.md  CONTRIBUTING.md  LICENSE  README.md  SECURITY.md
├── Dockerfile  compose.yaml  langgraph.json  Makefile
└── pyproject.toml  uv.lock
```

### 13.2 Tooling

- **uv** for everything, with the lockfile committed and dependency groups `dev`, `test` and `evals`.
- **ruff** for linting and formatting, and **mypy** in strict mode.
- **pytest with pytest-asyncio.** Warnings count as errors, with a documented list of filters. Coverage is reported.
- **pre-commit:** ruff, mypy, gitleaks, a lockfile check and basic hygiene hooks.
- **Makefile targets:** `setup`, `studio`, `up`, `down`, `migrate`, `incident`, `test`, `test-int`, `test-e2e`, `lint`, `fmt`, `typecheck`, `eval`, `rename`.

### 13.3 CI (GitHub Actions)

**Jobs:**
- lint and typecheck;
- unit tests on 3.14;
- integration tests with Postgres 18 and ElasticMQ service containers;
- end-to-end tests: the `kill -9` resume and SIGTERM drain tests against a worker subprocess;
- a Docker build plus a migration smoke test;
- the removal job;
- evals (a separate workflow, gated on secrets).

**Workflow settings:**
- actions pinned to a commit SHA;
- minimal `permissions:`;
- concurrency groups;
- a uv cache.

### 13.4 Dependencies

Dependabot runs weekly for Python packages and GitHub Actions, with the LangChain-family packages grouped and a cooldown of about 7 days.

### 13.5 Container and Compose

- **Dockerfile:**
  - multi-stage, with a uv build stage (`uv sync --frozen --no-dev`, bytecode compiled);
  - runtime image `python:3.14-slim`, with `ripgrep`, `git` and `ca-certificates` installed from apt;
  - a non-root user;
  - `STOPSIGNAL SIGTERM`.
- **compose.yaml:** `postgres` (with a healthcheck) → `elasticmq` (input queue, DLQ and result queue) → `migrate` (runs once) → `worker`. `make incident` sends a fixture message.

### 13.6 Configuration

- **pydantic-settings:** one `Settings` class with the `APP_` prefix. Vendor-standard names keep their usual form: `DD_*`, `LANGSMITH_*`, `LANGGRAPH_*` and the AWS variables.
- **`.env.example`** documents every variable.
- **`APP_ENV`** is `dev`, `test` or `prod`. In `prod`, a missing secret stops startup.

### 13.7 Governance and docs

- **License:** MIT.
- **README:** short, with a pitch, what's inside, a three-command quickstart, a "real mode on EKS" section, and the practices list.
- **Project files:** CONTRIBUTING, the Contributor Covenant, SECURITY.md, issue and pull-request templates.
- **CHANGELOG:** Keep a Changelog format, with SemVer tags. Versions stay at 0.x until launch.

### 13.8 AGENTS.md

`AGENTS.md` holds the project map, the commands and the rules:

- Every external system is reached through an adapter interface that has a fake.
- Steps are async, and every side effect is idempotent.
- No secrets in state, in `configurable` or in traces.
- Evidence items have IDs, and results cite only those IDs.
- Every new practice needs a test and a note.
- It shows how to add an evidence collector, a language, or an agent.

`CLAUDE.md` imports `AGENTS.md`.

### 13.9 Removable parts

_(Designer's call.)_

- **The example agent:** delete `agents/incident_triage/` and remove its lines in `langgraph.json` and `worker/registry.py`. `scripts/remove_example.py` does this, and CI proves it works.
- **The worker:** delete `worker/` and the `worker` and `elasticmq` services in Compose. A fork does this when it only wants Studio or a different trigger.

### 13.10 Sample apps (fixtures)

Each sample is a few source files with a known bug, a realistic crash log in that language's real trace format, an incident message, recorded Kubernetes and Datadog responses, and the expected result. There are twelve:

- JavaScript (Node.js) and TypeScript;
- Python;
- Java, both Spring Boot and plain;
- Go;
- PHP;
- Ruby;
- Kotlin;
- Scala;
- C and C++.

The infrastructure fixtures have no sample app.

## 14. Phases

- **Phase 0, scaffolding:** §13.1–§13.8 in place, with CI green on a minimal package.
- **Phase 1, the worker and Incident Triage:** everything in §7–§12.1 and §12.3, delivered through real and fake adapters and backed by the practices list. It ends with a tagged v0.1.0 and a manual smoke test in real mode on the owner's AWS account.
- **Phase 2, open-source observability:** §12.2.
- **Phase 3, launch:**
  - v1.0.0;
  - a 30-second crash-and-resume video;
  - a pitch to @LangChainAI;
  - awesome-lists;
  - a docs site;
  - Copier updates.
- **Backlog:** §4.

## 15. Definition of done (Phases 0–1)

- **No-credential run:** with no credentials, `docker compose up` followed by `make incident LANG=<lang>` produces a valid `TriageResult` in Postgres and in the log. This works for all twelve sample apps and all infrastructure fixtures.
- **Studio:** `make studio` shows the graph, and a fixture incident runs end to end.
- **Resume:** a `kill -9` of the worker during the Datadog step is followed by a restart. The run resumes at the Datadog step, Kubernetes evidence isn't collected again, and the result is published once.
- **Drain:** SIGTERM drains the run after its current step, and another worker resumes it.
- **Duplicates:** a duplicate message produces one run.
- **Bad messages:** a bad message goes to the DLQ.
- **CI:** all required jobs are green.
- **Practices:** every practice in §11 has code, a test and a note.
- **Real mode:** a manual check with the owner's AWS, EKS, Datadog and Git access produces a valid result for a real crashed pod. This is documented in `docs/guides/real-mode.md`.

## 16. Owner decisions

1. **Name:** `langgraph-and-observability`.
2. **First agent:** Incident Triage (2026-11-02). Support Desk is in the backlog, with its earlier decisions kept:
   - the customer confirms actions;
   - Opus 5.5 is the default model.
3. **Runtime:**
   - it runs on EKS, and the owner handles scaling;
   - SQS triggers runs;
   - everything is persisted to Postgres;
   - a run resumes at the failed step after a restart;
   - the owner has a 60-second shutdown hook.
4. **Evidence order:**
   1. Kubernetes logs and evidence;
   2. Datadog events;
   3. code evidence (tree-sitter and ripgrep), when the first two don't explain the failure.
5. **Model:**
   - Bedrock through boto3;
   - typed structured JSON replies;
   - Claude Opus 5.5 as the default model.
6. **Languages:** the most heavily used server-app languages, except Rust and .NET, as listed in §9.6.
7. **Testing** uses simulated data and sample apps.
8. **Building:** Claude Opus 5.5 writes the code and Claude Fable 5.1 reviews it. The implementation runs in a new session from the plan.
9. **Observability:** Langfuse and Phoenix come in Phase 2.

## 17. Left to the builder

- **Agent details:** prompt wording, the exact evidence caps, and the per-language tables of definition node types.
- **Bedrock request shape:** the exact structured-output request shape for boto3, verified against AWS documentation.
- **Repository fetching:** git fetch details per Git host, and whether fetching by SHA is allowed.
- **Migration runner:** the implementation details.
- **Test tooling:** whether integration tests use testcontainers or CI service containers locally.

## Appendix A: research summary

| Production problem | Where this design answers it |
|---|---|
| Connections drop silently; PgBouncer breaks the defaults | §7.1 |
| A dead database still passes the health check | §8.6, P5 |
| A shared saver handles checkpoint I/O one call at a time | §7.2 |
| Checkpoint tables grow forever | §7.4 |
| Deserialization CVEs | §7.2 |
| A resumed run repeats finished work | §8.2–§8.4, P11 |
| The retry default is both too narrow and too broad | §6.5 |
| `configurable` leaks into traces | §8.5 |
| Trace size limits (about 20 MB) | §8.5, §12.1 |
| Fake chat models can't bind tools (not used: the agent calls Bedrock directly) | §9.7 recorded mode |

**Growth findings:**

- A working agent beats a skeleton by 10–50×.
- A repost by @LangChainAI was the biggest single boost for a template.
- Spikes come from borrowed audiences and news moments.
- Major releases act as relaunches.
- Visible maintenance keeps growth going.
- The README is basic hygiene, not what drives growth.

## Appendix B: sources

- **Versions and compatibility:**
  - [langgraph (PyPI)](https://pypi.org/project/langgraph/)
  - [LangGraph CI matrix](https://github.com/langchain-ai/langgraph/blob/main/.github/workflows/_test.yml)
  - [Python releases](https://www.python.org/downloads/)
- **LangGraph changes:**
  - [LangGraph releases](https://releases.sh/langchain/langgraph-releases.md)
  - [LangChain/LangGraph in late 2026](https://www.developersdigest.tech/blog/langchain-langgraph-whats-new-2026)
- **Persistence:**
  - [Persistence docs](https://docs.langchain.com/oss/python/langgraph/add-memory)
  - [Concurrent FastAPI requests](https://forum.langchain.com/t/does-the-postgres-checkpointer-serialize-concurrent-fastapi-requests/2882)
  - [langgraph #3716](https://github.com/langchain-ai/langgraph/issues/3716)
  - [Resume re-runs](https://forum.langchain.com/t/pre-interrupt-code-re-runs-on-resume-anti-pattern-or-is-there-a-sanctioned-way-to-detect-resume/3809)
- **Languages:**
  - [GitHub Octoverse 2025](https://github.blog/news-insights/octoverse/octoverse-a-new-developer-joins-github-every-second-as-ai-leads-typescript-to-1/)
  - [RedMonk January 2026](https://redmonk.com/sogrady/2026/04/14/language-rankings-1-26/)
- **Local SQS:** [ElasticMQ](https://github.com/softwaremill/elasticmq)
- **Observability:**
  - [LangSmith + OpenTelemetry](https://docs.langchain.com/langsmith/trace-with-opentelemetry)
  - [Langfuse OpenTelemetry](https://langfuse.com/integrations/native/opentelemetry)
  - [Phoenix license](https://arize.com/docs/phoenix/self-hosting/license)
- **Growth:**
  - [@LangChainAI repost](https://x.com/LangChainAI/status/1911509695686193277)
  - [HN launch study](https://arxiv.org/abs/2511.04453)
- **Verified in installed source (2026-11-01/02):**
  - `langgraph/checkpoint/postgres/aio.py` (`_cursor` lock)
  - `langgraph/store/postgres/aio.py` (pool-aware lock)
  - `langchain_core/runnables/config.py` (`configurable` copied into traces)
  - `langgraph/_internal/_retry.py`
  - `langgraph/checkpoint/serde/jsonplus.py` (`LANGGRAPH_STRICT_MSGPACK`)
