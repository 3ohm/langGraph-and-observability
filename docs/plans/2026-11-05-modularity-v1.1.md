# Modularity v1.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the template easy for anyone to reuse for their own agent with Postgres persistence and LangSmith observability: no silent success on an interrupt, a trigger without SQS, the LangGraph Store, a recipe for your own host, and a core that carries no example-agent knowledge after `scripts/remove_example.py`.

**Architecture:** Small explicit seams, no new layer (ADR 0001). Two optional `AgentSpec` fields move agent knowledge out of core (`required_settings`, `trace_metadata`); `default_retry_policy` takes a `retry_on` argument; `persistence/store.py` mirrors `make_checkpointer`; example-only settings, `.env.example` lines and test defaults go between `# example-agent:begin` / `# example-agent:end` markers.

**Tech Stack:** Python 3.14, uv, LangGraph 1.2.19, langgraph-checkpoint-postgres 3.2.0 (`AsyncPostgresSaver`, `AsyncPostgresStore`), psycopg 3.3 pool, pydantic-settings, pytest, ruff, pylint, mypy (strict).

**Spec:** `docs/design/2026-11-01-langgraph-and-observability-design.md` (§7 persistence, §8 worker, §12 observability, §13.9 removal), plus the owner's decisions of 2026-11-05 below, which come from a modularity review of the template.

## Owner decisions (2026-11-05)

- **In scope:**
  - F1, the interrupt guard and the docs
  - F3, the Postgres Store
  - F4, the core decoupling
  - F5, the hosting recipe
  - F6, the trigger docs and `runs create`
  - F8, `session.id`
  - F9, the names that `rename.py` keeps
  - F10, the pool global
- **Out of scope:**
  - F2, a skeleton "hello" agent
  - F7, a script that removes the worker
  - a `waiting` run status and the resume of an interrupted run in the worker. These wait for the Support Desk design: chat agents can run on `langgraph dev` or the Agent Server.
- **Dependencies:** the example's dependencies stay in `pyproject.toml`. `remove_example.py` prints the `uv remove` command for them. It does not edit the lock file.
- **Interrupts:** an interrupt is a failed attempt, and it gets a retry like any other error, up to `APP_MAX_ATTEMPTS`. The recovery loop runs it again. LangGraph then runs only the interrupted node again: the steps that completed do not run again. So a node whose interrupt condition has cleared completes on a later attempt. A node that interrupts every time uses all the attempts, then the run fails. It never ends as `success`.

## Global Constraints

- Python 3.14 only. No `from __future__ import annotations`. `uuid.uuid7()` for time-ordered IDs.
- uv only (`uv add`, `uv run`); commit `uv.lock`; CI sets `UV_LOCKED=1`.
- `make lint typecheck test` must pass: ruff, pylint (both runs of `make pylint`), mypy strict. An inline `# pylint: disable=<name>` needs a short reason; a stale one fails (`useless-suppression`).
- AGENTS.md rules 1–7 hold. Vendor SDK imports only where rule 1 allows. Rule 3: no secrets in state, `configurable`, logs, traces, prompts, or the Store. Rule 6: no helper that hides a LangGraph API. Rule 7: an example-only part sits between `# example-agent:begin` and `# example-agent:end`, and `remove_example.py` stays in step.
- Unit tests: no Docker, no network, no credentials. Postgres tests are marked `integration`.
- A practice note has the sections Problem, What the template does, Where, Proof, in at most 40 lines (`tests/unit/test_docs.py`).
- Docs in normal English. Commits: Conventional Commits, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Do not push.
- `deploy/sql/roles.sql` needs no change: its default privileges give `lps_app` row access to every table that `lps_migrator` creates later, the Store's tables included.

## Review Focus

1. A graph that interrupts must never end `success`. Each interrupt is a failed attempt with a retry, up to `APP_MAX_ATTEMPTS`. A retry runs only the interrupted node again, never a step that completed — Task 1.
2. `runs create` with input that fails validation must not print the input's values (they can hold secrets) — Task 2.
3. `migrate` twice in a row must succeed with the Store's `setup()` in it — Task 3.
4. The Store must work on a PgBouncer-mode pool (`prepare_threshold=None`) — Task 3.
5. After `remove_example.py`, a `prod` worker must start with only `APP_DATABASE_URL` set: no SQS, no Datadog keys — Tasks 6 and 7.

---

### Task 1: An interrupt is a failed attempt with a retry; the worker is a batch host (F1)

**Files:**
- Modify: `src/langgraph_and_observability/worker/executor.py` (`_invoke`, `_execute`)
- Modify: `src/langgraph_and_observability/agent_spec.py` (module docstring)
- Modify: `docs/guides/customize.md` (`## Add an agent`)
- Test: `tests/integration/test_executor.py`, `tests/unit/test_upstream_shape.py`

**Interfaces:**
- Produces: `class InterruptNotSupportedError(RuntimeError)` in `worker/executor.py`, with the message `"the graph stopped at an interrupt; this worker gives no resume value, so the attempt fails and the run gets a retry up to APP_MAX_ATTEMPTS; an interactive graph runs on langgraph dev or the LangGraph Agent Server"`. The message also names the nodes that wait. In LangGraph 1.x an `Interrupt` holds only `id` and `value`, so take the node names from `(await graph.aget_state(config)).next`, which is a public API and costs one checkpoint read, only for an interrupt. The message never names an interrupt's value, which can hold data from the state.

- [ ] **Step 1: Pin LangGraph's result shape.** In `tests/unit/test_upstream_shape.py` add `test_ainvoke_result_names_an_interrupt`: a one-node `StateGraph` whose async node calls `langgraph.types.interrupt("need input")`, compiled with `InMemorySaver`, `ainvoke(..., durability="sync")` returns a `dict` that has the key `"__interrupt__"`. (LangGraph 1.x made the `INTERRUPT` constant private; the documented key is the literal.)
- [ ] **Step 2: Write the failing executor tests.** In `tests/integration/test_executor.py`, use specs made with `dataclasses.replace(STEPS_SPEC, name=..., build_graph=...)`, and claim the run again before each `execute`, as the recovery loop does.
  - `test_interrupt_is_a_failed_attempt_with_a_retry`: the graph interrupts every time. `max_attempts=3`.
    - The first two `execute` calls return `RunStatus.PENDING`.
    - The third returns `RunStatus.FAILED`.
    - The row has `attempt == 3`, `error["type"] == "InterruptNotSupportedError"`, and `"langgraph dev"` in `error["message"]`.
    - The row never has the status `success`.
  - `test_retry_after_an_interrupt_completes_when_the_condition_clears`: the graph is `first` → `gate`. `gate` interrupts while a test flag is false.
    - The first `execute` returns PENDING.
    - Set the flag. The second `execute` returns SUCCESS.
    - A call counter shows that `first` ran one time and `gate` ran two times.
- [ ] **Step 3: Run them.** `uv run pytest -m integration tests/integration/test_executor.py -k interrupt -v` → FAIL (today the first `execute` returns SUCCESS).
- [ ] **Step 4: Implement.** In `_invoke`, keep the result of `ainvoke`. Raise `InterruptNotSupportedError` when the result is a `Mapping` with the key `_INTERRUPT_KEY = "__interrupt__"`. Make that key a module constant, with a comment that points at the upstream-shape test. The existing `except Exception` in `_execute` then records the attempt through `_mark_error` with `settings.max_attempts`, as for any other error. That gives PENDING (recovery retries the run) or FAILED (the last attempt). Add no other code path. Update the module docstring's outcome list: "the graph stops at an interrupt: a failed attempt (`mark_error`)".
- [ ] **Step 5: Docs.** Update the `agent_spec.py` docstring and `customize.md` (`## Add an agent`). They say that the worker is a batch host: one invocation per attempt, with no resume value and no streaming to a client. An interrupt is a failed attempt and gets a retry. A retry runs the interrupted node again, so a node can use an interrupt to wait for a condition, such as an approval in another system. An interactive graph runs on `langgraph dev` or the Agent Server.
- [ ] **Step 6: Verify.** `make lint typecheck test` and `uv run pytest -m integration tests/integration/test_executor.py` → PASS.
- [ ] **Step 7: Commit.** `fix(worker): an interrupt is a failed attempt with a retry, never a success`

### Task 2: Start a run without SQS: `runs create` and the trigger docs (F6)

**Files:**
- Modify: `src/langgraph_and_observability/worker/cli.py`
- Modify: `docs/guides/customize.md` (new `## Other triggers`, after `## Add an agent`)
- Test: `tests/integration/test_cli.py`

**Interfaces:**
- Consumes: `RunRepo.create(agent, dedup_key, input) -> tuple[UUID, bool]`; `load_agents(settings)`; `AgentSpec.input_model`, `AgentSpec.dedup_key(model, sent_at)`.
- Produces: the command `python -m langgraph_and_observability runs create AGENT --input JSON [--dedup-key KEY]`. Stdout: one JSON line `{"run_id": ..., "created": true|false, "dedup_key": ...}`, exit 0. Errors on stderr, one JSON line, exit 1: `{"error": "unknown agent", "agent": ...}`, or `{"error": "invalid input", "details": [...]}`, where each detail is `"<loc>: <msg>"` with no input value (the same rule as `executor.error_fields`). A JSON syntax error in `--input` is a usage error, exit 2.

- [ ] **Step 1: Write the failing tests** in `tests/integration/test_cli.py` (with the steps agent registered the way the file's other tests register it):
  - `test_runs_create_adds_a_pending_run`: exit 0; the run is `pending` with the given input; `created` is true.
  - `test_runs_create_twice_is_one_run`: the same `--dedup-key` twice gives the same `run_id`, and `created` is false the second time.
  - `test_runs_create_rejects_bad_input_without_its_values`: the input holds a field value `"sk-live-SECRET123"` that fails validation; exit 1; stderr has `"invalid input"` and not `SECRET123`; no run is added.
  - `test_runs_create_unknown_agent`: exit 1, `"unknown agent"`.
- [ ] **Step 2: Run them.** `uv run pytest -m integration tests/integration/test_cli.py -k create -v` → FAIL (no `create` action).
- [ ] **Step 3: Implement `_create(repo, args) -> int`** and its parser entry. Without `--dedup-key`, use `spec.dedup_key(model, datetime.now(UTC))`. Store `model.model_dump(mode="json")`, as intake does. Update the module docstring's command list.
- [ ] **Step 4: Docs.** In `customize.md`, add `## Other triggers`. It says that the `runs` table is the queue: SQS intake writes a row and starts it at once. Any other producer, such as an HTTP route, a CronJob or `runs create`, writes a row with `RunRepo.create`, and the worker's recovery loop claims the row within `APP_RECOVERY_INTERVAL_SECONDS`. With no `APP_SQS_QUEUE_URL`, the worker runs without intake. Add a ten-line FastAPI route example that calls `RunRepo.create`.
- [ ] **Step 5: Verify.** `make lint typecheck test`, `uv run pytest -m integration tests/integration/test_cli.py` → PASS.
- [ ] **Step 6: Commit.** `feat(cli): runs create, a trigger without SQS`

### Task 3: The LangGraph Store on Postgres (F3)

**Files:**
- Create: `src/langgraph_and_observability/persistence/store.py`
- Modify: `persistence/migrate.py` (the `langgraph` component), `worker/executor.py`, `worker/main.py` (`_start`)
- Create: `docs/practices/P24.md`; Modify: `README.md` (practice table, "What's inside"), `AGENTS.md` (project map), `tests/unit/test_docs.py` (`PRACTICE_IDS = [f"P{n}" for n in range(1, 25)]`), the design doc (§7: the Store is no longer backlog), `CHANGELOG.md` (0.1.0 Added)
- Test: `tests/integration/test_store.py` (new), `tests/integration/test_migrate.py`, `tests/integration/test_executor.py`

**Interfaces:**
- Produces: `make_store(pool: AsyncConnectionPool[AsyncConnection[DictRow]]) -> AsyncPostgresStore` (from `langgraph.store.postgres.aio`), which returns `AsyncPostgresStore(pool)`. No index and no TTL; its docstring says how to add `index=` (pgvector is in the Compose image) or `ttl=`, and that the Store's values are JSON in Postgres: no serde allowlist, no encryption at rest, and never a secret (rule 3).
- Produces: `RunExecutor(..., store: BaseStore | None = None)`; it compiles each graph with `compile(checkpointer=saver, store=store)`.
- Produces: `run_migrate` runs `await AsyncPostgresStore(conn).setup()` right after `AsyncPostgresSaver(conn).setup()`, in the `langgraph` component, on the same autocommit connection.

- [ ] **Step 1: Write the failing tests.**
  - `tests/integration/test_store.py::test_store_keeps_memory_across_threads`: a graph whose node reads `runtime.store` item `("memory", "user-1")`/`"count"`, adds 1 and writes it back; compiled with `make_store(pool)` after `run_migrate`; two `ainvoke` calls on two different `thread_id`s; the second returns count 2.
  - `tests/integration/test_store.py::test_store_works_in_pgbouncer_mode`: the same round trip on a pool from `create_pool(settings)` with `pgbouncer_mode=True`.
  - `tests/integration/test_migrate.py::test_migrate_twice_sets_up_the_store`: `run_migrate` twice; no error; the table `store` exists.
  - `tests/integration/test_executor.py::test_executor_compiles_graphs_with_the_store`: an `InMemoryStore` passed to `RunExecutor`; a node that puts an item; after `execute`, the item is in the store.
- [ ] **Step 2: Run them.** `uv run pytest -m integration tests/integration/test_store.py tests/integration/test_migrate.py tests/integration/test_executor.py -v` → FAIL.
- [ ] **Step 3: Implement** `make_store`, the `setup()` call, the executor argument, and in `Worker._start` `store = make_store(pool)` passed to `RunExecutor`.
- [ ] **Step 4: Docs.** `P24.md`, "Long-term memory in the Postgres Store". It says:
  - nodes use `runtime.store`;
  - `migrate` creates the tables;
  - `langgraph dev` and the Agent Server bring their own store on their path;
  - the Store keeps no secrets.

  Add the README row and "What's inside" text, and the AGENTS.md map entry (`persistence/`: "... checkpointer, store, action ledger"). In the design doc, change the backlog line. Add a CHANGELOG line.
- [ ] **Step 5: Verify.** `make lint typecheck test`, `make test-int` → PASS.
- [ ] **Step 6: Commit.** `feat(persistence): the LangGraph Store on the same Postgres pool (P24)`

### Task 4: A recipe to use the persistence and tracing in your own process (F5)

**Files:**
- Modify: `docs/guides/customize.md` (new `## Use the persistence and tracing in your own process`, before `## Run without the queue worker`)
- Test: `tests/unit/test_docs.py`, `tests/integration/test_host_recipe.py` (new)

**Interfaces:**
- Consumes: `configure_tracing`, `close_tracing`, `create_pool`, `open_pool`, `make_serde`, `make_checkpointer`, `make_store` (Task 3), `RunInfo`, `AgentSpec`: the same calls, in the same order, as `Worker._start` in `worker/main.py`.
- Produces: one fenced `python` block between the lines `<!-- host-recipe:begin -->` and `<!-- host-recipe:end -->` that defines `async def run_once(spec: AgentSpec, payload: Mapping[str, Any], *, settings: Settings, thread_id: str) -> dict[str, Any]`. It opens the pool, builds the saver and the store, compiles `spec.build_graph()`, invokes with `{"configurable": {"thread_id": thread_id}}`, `context=spec.make_context(...)` and `durability="sync"`, and closes the pool and tracing in `finally`. One sentence after the block says what `langgraph dev` and LangSmith Deployment replace on their path (their own checkpointer and store, so not the strict serde, the encryption or the pool-aware saver).

- [ ] **Step 1: Write the failing tests.**
  - `tests/unit/test_docs.py::test_host_recipe_compiles`: the block exists once, and `compile(code, "customize.md", "exec")` succeeds.
  - `tests/integration/test_host_recipe.py::test_host_recipe_runs_the_steps_graph`: `exec` the block into a fresh namespace; `await namespace["run_once"](STEPS_SPEC, <a valid steps input>, settings=<test settings>, thread_id=str(uuid.uuid7()))` returns the final state; a saver on the test database has a checkpoint for that `thread_id`.
- [ ] **Step 2: Run them** → FAIL (no block).
- [ ] **Step 3: Write the section and the block.**
- [ ] **Step 4: Verify.** `make lint typecheck test`, `uv run pytest -m integration tests/integration/test_host_recipe.py` → PASS.
- [ ] **Step 5: Commit.** `docs(customize): run the graph in your own process with the same persistence and tracing`

### Task 5: Core retry policy without vendor types (F4, part 1)

**Files:**
- Modify: `src/langgraph_and_observability/policies.py`
- Create: `src/langgraph_and_observability/agents/incident_triage/policies.py`
- Modify: `agents/incident_triage/graph.py`, and the example's tests and adapter docstrings that import or name `policies.is_transient`
- Test: `tests/unit/test_policies.py`; create `agents/incident_triage/tests/test_policies.py`

**Interfaces:**
- Produces (core): `is_transient(exc: BaseException) -> bool` accepts only `ConnectionError`, `TimeoutError`, `socket.gaierror`, `ssl.SSLEOFError`, `NodeTimeoutError` and the psycopg types it has now. `default_retry_policy(retry_on: Callable[[BaseException], bool] = is_transient) -> RetryPolicy`: the same numbers as today. Core imports no `aiohttp`, `aiosonic` or `kubernetes_asyncio`.
- Produces (example): `agents.incident_triage.policies.is_transient(exc) -> bool`. It returns true for what the core accepts, plus `aiohttp.ClientConnectionError`, the aiosonic types (with `WebSocketMessageTooBig` excluded), and a Kubernetes `ApiException` with status 429 or 5xx. The vendor comments move with it. The example's `graph.py` uses `default_retry_policy(retry_on=is_transient)` with the example's function.

- [ ] **Step 1: Move the tests.** The vendor cases of `tests/unit/test_policies.py` go to the example's `test_policies.py`, against the example's function. The core file gets `test_core_policies_import_no_vendor_sdk`: in an AST of `policies.py`, no import of those three packages. It also gets `test_default_retry_policy_takes_retry_on`.
- [ ] **Step 2: Run them** → FAIL.
- [ ] **Step 3: Implement**, then point the example's tests and docstrings at the example's `is_transient`.
- [ ] **Step 4: Verify.** `make lint typecheck test` → PASS. Also run `make test-e2e`, because the example's retries changed module.
- [ ] **Step 5: Commit.** `refactor(policies): vendor transient types move into the example agent`

### Task 6: Agents own their settings check and trace metadata; `session.id` (F4 part 2, F8)

**Files:**
- Modify: `agent_spec.py`, `settings.py` (`require_runtime_secrets`), `worker/main.py`, `worker/executor.py`, `observability/metadata.py`, `agents/incident_triage/spec.py` (and a new `agents/incident_triage/tracing.py` if the metadata function needs a home)
- Test: `tests/unit/test_settings.py`, `tests/unit/test_worker_main.py`, `tests/unit/test_tracing.py`, the example's tests, `tests/integration/test_executor.py`

**Interfaces:**
- Produces, in `AgentSpec` (both optional, default `None`; add them to the docstring):
  - `required_settings: Callable[[Settings], Sequence[str]] | None`. It names the settings fields that the agent needs in `prod`.
  - `trace_metadata: Callable[[Mapping[str, Any], Settings], Mapping[str, str]] | None`. It gives the agent's trace keys from the run's stored input.
- Produces: `Settings.require_runtime_secrets()` in `prod` requires `database_url`, and `sqs_queue_url` only when `intake_agent` is not empty. It no longer names Datadog. The `full`-content rule does not change.
- Produces: `check_agent_settings(settings: Settings, agents: Mapping[str, AgentSpec]) -> None` in `worker/main.py`. In `prod`, it raises `MissingSecretsError` that names each field that an agent's `required_settings` lists and that is `None`. It is called right after `require_runtime_secrets()`.
- Produces: `run_metadata` keys: `run_id`, `session.id` (the same value as `run_id`, which is also the `thread_id`), `dedup_key`, `agent`, `env`, `app_version`, `git_sha`. No `cluster`, `namespace` or `workload`. The executor merges `spec.trace_metadata(run.input, settings)` under them, so a core key wins.
- Produces (example): the example's `required_settings` returns `["dd_api_key", "dd_app_key"]` when `settings.adapters == "real"`, else `[]`. Its `trace_metadata` returns `cluster` (from `settings.cluster_name`), plus `namespace` and `workload`, with today's rules.

- [ ] **Step 1: Write the failing tests.**
  - settings: `prod` with `intake_agent=""` and only `database_url` passes; `prod` with an intake agent and no queue URL raises and names `sqs_queue_url`; `adapters="real"` without DD keys passes `require_runtime_secrets`.
  - worker main: `check_agent_settings` names the DD keys for the example in `prod` with real adapters; it is quiet in `dev`.
  - tracing: `run_metadata` has `session.id` and no `cluster`; the example's `trace_metadata` keeps today's `namespace`, `workload` and `cluster` cases (move them from `test_tracing.py`).
  - executor (integration): the graph config's metadata holds the agent keys and the core keys, and a core key wins over an agent key of the same name.
- [ ] **Step 2: Run them** → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Verify.** `make lint typecheck test`, `make test-int`, `make test-e2e` → PASS.
- [ ] **Step 5: Commit.** `refactor: agents own their prod settings check and trace metadata; add session.id`

### Task 7: Example settings, `.env.example` lines and test defaults leave with the example (F4 part 3)

**Files:**
- Modify: `settings.py`, `.env.example`, `tests/unit/test_settings.py` (`EXPECTED_DEFAULTS`), `scripts/remove_example.py` (`MARKED_FILES`, the `Edit` list, the final message), `docs/guides/customize.md` (`## Remove the example agent`)
- Test: `tests/unit/test_scripts.py`, create `tests/unit/test_core_imports.py`

**Interfaces:**
- Consumes: Tasks 5 and 6. After them, no core module reads an example setting.
- Produces:
  - Markers: the example-only fields of `Settings`, their `.env.example` sections and their `EXPECTED_DEFAULTS` entries sit between `# example-agent:begin` and `# example-agent:end`. These fields are `adapters`, `llm_mode`, `bedrock_model_id`, `judge_model_id`, `model_effort`, `max_output_tokens`, `cluster_name`, `log_tail_lines`, `dd_*`, `git_*`, `code_budget_chars`, `source_*`, `grammar_dir`, `fixtures_dir`, `studio_fixture` and `fake_*`. The vendor samples that the example alone needs (`KUBECONFIG`) go inside the markers too.
  - `MARKED_FILES` adds `src/<pkg>/settings.py` (built from the script's package constant, so it stays rename-safe), `.env.example` and `tests/unit/test_settings.py`.
  - `Edit` entries whose text is now inside a marked part are deleted. The `APP_INTAKE_AGENT` and `intake_agent` default edits stay.
  - `EXAMPLE_DEPENDENCIES = ("kubernetes-asyncio", "datadog-api-client", "tree-sitter", "tree-sitter-language-pack", "aiohttp")` in `remove_example.py`. After a removal, the script prints `uv remove ` followed by these names, as its next step.

- [ ] **Step 1: Write the failing tests.**
  - `tests/unit/test_core_imports.py::test_core_imports_no_example_dependency`: in an AST of every module under `src/<pkg>/` outside `agents/incident_triage/`, there is no import of `kubernetes_asyncio`, `datadog_api_client`, `aiosonic`, `aiohttp`, `tree_sitter` or `tree_sitter_language_pack`.
  - `tests/unit/test_scripts.py::test_example_dependencies_are_project_dependencies`: each name is in `pyproject.toml`'s `[project] dependencies`.
  - Extend `test_remove_example_in_temp_copy`:
    - In the copy, `settings.py` and `.env.example` have no `dd_api_key` / `APP_DD_` and no `fixtures_dir` / `APP_FIXTURES_DIR`.
    - The copy's unit test collection still succeeds.
    - Stdout has the `uv remove` line.
    - In the copy, `Settings(env="prod", database_url="postgresql://x")` passes `require_runtime_secrets()` (Review Focus 5).
- [ ] **Step 2: Run them** → FAIL.
- [ ] **Step 3: Implement** the markers, the script changes and the `customize.md` text.
- [ ] **Step 4: Verify.**
  - Run `make lint typecheck test`.
  - Do the removal check by hand: `git archive HEAD | tar -x -C <empty temp dir>`, then in it `uv sync --all-groups`, `uv run scripts/remove_example.py`, `! grep -r incident_triage src`, `uv run pytest -m "not integration and not e2e"` and `make lint typecheck` → PASS.
- [ ] **Step 5: Commit.** `refactor: example-only settings, env lines and test defaults are removable parts`

### Task 8: Docs for the names that rename.py keeps and the pool global; multi-document YAML (F9, F10)

**Files:**
- Modify: `docs/guides/customize.md`, `.pre-commit-config.yaml`
- Test: `tests/unit/test_ci_config.py`

- [ ] **Step 1: Write the failing test.** `test_check_yaml_accepts_multiple_documents`: the `check-yaml` hook in `.pre-commit-config.yaml` has `--allow-multiple-documents` in its `args`. `deploy/k8s/rbac.yaml` is a multi-document file, and today the hook fails on it.
- [ ] **Step 2: Run it** → FAIL. **Step 3:** Add the argument.
- [ ] **Step 4: Docs.**
  - Under `## Rename the package`, list the short names that `rename.py` keeps:
    - the advisory lock keys `lps:migrate` and `lps:prune`. Keep them stable while one fleet runs, because a new pod must take the same lock as an old pod.
    - the database roles `lps_app` and `lps_migrator`
    - the annotation keys `lps.dev/source-repo` and `lps.dev/source-commit` (example)
    - `/tmp/lps-source-cache` (example)
    - `LPS_TEST_FAULTS` (tests)
  - Under `## Add an agent`, add one paragraph. A node that writes to Postgres takes the pool from `persistence.resources.current_pool()`. Without a pool, as in Studio, it must fall back and say so, as the example's `publish` node does.
- [ ] **Step 5: Verify.** `make lint typecheck test` → PASS.
- [ ] **Step 6: Commit.** `docs(customize): names that rename.py keeps, and the pool global; check-yaml accepts multi-document files`

---

After Task 8: a whole-branch review, then `make lint typecheck test test-int test-e2e`, then the CI removal check, then a push to PR #1 (only with the owner's agreement).
