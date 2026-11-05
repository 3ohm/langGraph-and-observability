# Customize

A fork usually does three things: renames the package, removes or keeps the example agent, and
adds its own agent. This guide also shows how to start runs without SQS, how to add an evidence
collector or a language to the example, how to run a graph in your own process, and how to run
without the queue worker. The rules that every change follows are in [AGENTS.md](../../AGENTS.md).

## Rename the package

```bash
uv run scripts/rename.py acme_agents --dry-run   # list what would change
uv run scripts/rename.py acme_agents
uv sync --all-groups                             # install the renamed package
make fmt                                         # the new name changes line lengths
make lint typecheck test
```

The new name is a lowercase Python identifier (letters and digits in words joined by single
underscores), not a keyword or a standard-library module, and not taken: neither a package in
`uv.lock` (such as `langgraph` or `psycopg_pool`) nor a module that Python can import already
(such as `yaml`). The script replaces
`langgraph_and_observability` with it, and `langgraph-and-observability` (the distribution, the
image and the Kubernetes names) with its hyphenated form, in every text file, and renames the
paths that hold either name. It skips `.git/`, `.venv/`, `.superpowers/`, caches, binary files
and `LICENSE`, whose copyright notice MIT asks every copy to keep. `uv.lock` stays valid. A
longer name can push a line past ruff's 100 characters; `make lint` shows where.

The script changes only those two names. It keeps these short names as they are; change them by
hand if you want other names:

- the advisory lock keys `lps:migrate` and `lps:prune`, which let only one `migrate` and one
  retention pass run at a time (P4, P6). Keep them stable while one fleet runs, because a new pod
  must take the same lock as an old pod.
- the database roles `lps_app` and `lps_migrator` (`deploy/sql/roles.sql`)
- the annotation keys `lps.dev/source-repo` and `lps.dev/source-commit` (example), the defaults of
  `APP_SOURCE_ANNOTATION_REPO_KEY` and `APP_SOURCE_ANNOTATION_COMMIT_KEY`
- `/tmp/lps-source-cache` (example), the default of `APP_SOURCE_CACHE_DIR`
- `LPS_TEST_FAULTS` (tests), the variable that gives the test graph its faults
  (`tests/graphs/steps.py`)

## Remove the example agent

```bash
uv run scripts/remove_example.py
make lint typecheck test
```

It deletes `src/<package>/agents/incident_triage/` and the files that only the example uses
(`scripts/send_incident.py`, `scripts/sync_dataset.py`, their tests, the agent's end-to-end test,
`.github/workflows/evals.yml` and `docs/guides/real-mode.md`, with its links in `README.md` and
the other docs). It
removes the agent's `langgraph.json` entry and registry line, resets the settings default that
names it (`APP_INTAKE_AGENT` becomes empty), and deletes the example's parts of the Makefile
(`grammars`, `incident`), the Dockerfile (the tree-sitter grammars), CI, `settings.py` (the
example's settings), `.env.example` (their lines), `tests/unit/test_settings.py` (their test
defaults), `compose.yaml` (the worker's simulated mode), and `deploy/k8s/configmap.yaml` and
`deployment.yaml` (their keys, secrets and source cache), which those files mark with
`# example-agent:begin` and `# example-agent:end` lines.
After the removal, a `prod` worker needs only `APP_DATABASE_URL`. It prints each change; a second
run changes nothing. On a first run it refuses, and changes nothing, when a file it must edit has
drifted from the text it expects, or when the removal would leave a Makefile recipe or CI step
calling a make target that it deletes. CI's `removal` job proves on every commit that the rest
still passes (P22).

Then:

- remove the example's dependencies with the `uv remove` command that the script prints (it
  leaves `pyproject.toml` and `uv.lock` as they are);
- update `README.md`, `AGENTS.md` and `docs/`, which still describe the example;
- trim what only the example needs: the read-only ClusterRole in `deploy/k8s/rbac.yaml`, and the
  Bedrock and ECR statements of `deploy/iam/policy.json`. JSON has no comments for the markers, so
  the IAM policy keeps those statements until you remove them (least privilege, P19);
- register your agent and set `APP_INTAKE_AGENT`: without an agent, a worker with
  `APP_SQS_QUEUE_URL` set refuses to start.

## Add an agent

An agent is a plain LangGraph `StateGraph` plus an `AgentSpec` (`agent_spec.py`): the functions
that the worker calls around the graph.

The worker is a batch host: it invokes the graph one time per attempt, with no resume value and
no streaming to a client. When the graph stops at an interrupt, the attempt fails and the run gets
a retry, up to `APP_MAX_ATTEMPTS`, as for any other error. A retry runs the interrupted node again;
the steps that completed do not run again. So a node can use an interrupt to wait for a condition,
such as an approval in another system: when the condition has cleared, a later attempt completes.
A node that interrupts every time uses all the attempts, and then the run fails. An interactive
graph, which waits for a person's answer, runs on `langgraph dev` or the LangGraph Agent Server.

1. Make a package `src/<package>/agents/<name>/` with:
   - `graph.py`: `build_graph()` returns the `StateGraph`, not compiled; the worker compiles it
     with its checkpointer. Every node is `async`. Give each node
     `retry_policy=default_retry_policy()` and `timeout=step_timeout(seconds)` from `policies.py`
     (P12). For Studio, also export `graph`, compiled without a checkpointer.
   - `state.py`, `context.py`, `schemas.py`: the state (typed, with capped lists), the
     `context_schema` that holds the run's fixed inputs and settings, and the Pydantic input
     model. Secrets stay in the context, never in state or `configurable` (P14).
   - `spec.py`: one `AgentSpec` with `name`, `build_graph`, `input_model`, `dedup_key`,
     `make_context`, `initial_state`, `serde_types` (every type the state stores, defined at
     module level, P7), and, optionally, `migrations_dir`, `retention_sql` (P6),
     `required_settings` (the settings fields that a `prod` worker needs for the agent: it does
     not start while one is not set) and `trace_metadata` (the agent's own trace metadata keys,
     with string values, never a secret).
   - `adapters/`: one `interface.py` (a `Protocol`) per external system, with a real adapter and
     a fake. Vendor SDKs are imported only there.
   - `migrations/`: `0001_<name>.sql` and so on, if the agent has tables. `migrate` applies them
     after the core and worker migrations.
   - `tests/`: pytest finds them (`testpaths` includes the agents folder), and they get the root
     `conftest.py`'s isolation.
2. Register it: add its spec to `builtin_agents()` in `worker/registry.py`, or, for code outside
   this package, name it in `APP_EXTRA_AGENTS` (`["my_pkg.graph:SPEC"]`). Add its graph to
   `langgraph.json` for Studio.
3. Set `APP_INTAKE_AGENT=<name>` for the agent that takes the queue's messages.
4. If it adds a practice, add its test and a note in `docs/practices/`.

A node that writes to Postgres with its own SQL, not through `runtime.store`, takes the pool from
`persistence.resources.current_pool()`. The worker registers its pool there at startup
(`set_pool`), and so does `run_once`, the hosting recipe below. Without a pool, as in Studio or
in a graph test without a database, `current_pool()` returns `None`: the node must fall back and
say so, as the example's `publish` node does. It then sends the result to the `log` sinks only,
and logs `publish.skipped` with the sinks that it skipped.

### The dedup key

The worker maps messages with one dedup key to one run (P8). The example's key is
`sha256(cluster | namespace | workload or pod | alert_id, or else the 15-minute bucket of
fired_at)`. With an `alert_id`, time is ignored: two messages with the same `alert_id` are one
run, however far apart. So the `alert_id` must identify one occurrence of an alert, such as the
alert event's ID, never a stable monitor ID; otherwise every later incident from that monitor is
dropped as a duplicate of the first. Your agent's `dedup_key` needs the same care.

## Other triggers

The `runs` table is the queue. SQS intake is one producer: it writes a row for each message and
starts the run at once. Any other producer, such as an HTTP route, a Kubernetes CronJob or the
`runs create` command, writes a row with `RunRepo.create` (`worker/runs.py`), and the worker's
recovery loop claims the row within `APP_RECOVERY_INTERVAL_SECONDS` (default 30), when a run slot
is free. With no `APP_SQS_QUEUE_URL`, the worker runs without intake. In `prod`, though, the
worker needs `APP_SQS_QUEUE_URL` while `APP_INTAKE_AGENT` names an agent. The template's default
names the example agent, and an empty `APP_INTAKE_AGENT=` keeps that default (an empty value means
"not set"). So a `prod` worker runs without SQS only when the default is empty:
`scripts/remove_example.py` empties it, and a fork that keeps the example can set
`intake_agent: str = ""` in `settings.py`.

```bash
python -m <package> runs create my_agent --input '{"service": "checkout"}'
# {"run_id": "...", "created": true, "dedup_key": "..."}
```

`runs create` validates the input with the agent's input model and stores what the model dumps,
as intake does. Without `--dedup-key`, the agent's `dedup_key` gives the key, from the input and
the current time. A key that already has a run adds no run: the command prints that run's ID with
`"created": false`. Input that fails validation exits 1 with `"invalid input"` and, for each
failure, where it is and why, never the value. A route in your own web app does the same; here
`pool` is a pool from `create_pool` and `open_pool` (`persistence/pool.py`), opened at startup:

```python
from datetime import UTC, datetime

from fastapi import FastAPI

from <package>.agents.my_agent.spec import MyInput, spec
from <package>.worker.runs import RunRepo

app = FastAPI()


@app.post("/runs", status_code=202)
async def start_run(body: MyInput) -> dict[str, str | bool]:
    key = spec.dedup_key(body, datetime.now(UTC))
    run_id, created = await RunRepo(pool).create(spec.name, key, body.model_dump(mode="json"))
    return {"run_id": str(run_id), "created": created}
```

## Add an evidence collector to the example

1. Add an adapter package under `agents/incident_triage/adapters/<system>/`: `interface.py` (a
   `Protocol`), `real.py` and `fake.py`. The fake serves a new file in each incident fixture
   (`fixtures/FORMAT.md`). Add the adapter to `Adapters` and to `build_adapters`, which picks
   the real or the fake one by `APP_ADAPTERS`.
2. Add the step in `nodes/<system>.py`: an `async` function that reads its adapter from the
   context (`ctx(runtime, message)`) and returns a capped list of items under its own state key.
   Build each item with `evidence.make_item`, which redacts and caps it, and an ID from
   `evidence.make_id` (P15, P17). Add the kind to `EvidenceItem.kind` and the ID prefix to
   `EVIDENCE_ID_PATTERN` in `schemas.py`.
3. Wire it in `graph.py`: a node with its timeout in `TIMEOUTS`, an edge, and, when the run can
   go on without it, an error handler that adds a note, as `collect_datadog` has.
4. Show the items to the model and let it cite them: a section in
   `prompts.render_user_message`, and the new state key in the collected IDs of
   `nodes/analyze.py` and of the citation evaluator (`evals/evaluators.py`). A prompt change
   changes `PROMPT_VERSION`, so record the replies again.
5. Test the step on the fixtures, and run `make test` (the evals run there too, P21).

## Add a language to the example

1. Grammar: add a `LanguageSpec` to `LANGUAGES` in `codeintel/languages.py` (file extensions and
   definition node types) for a grammar that tree-sitter-language-pack ships. `make grammars`
   and the image build fetch every grammar of `LANGUAGES`, so run `make grammars` again. Test
   `extract.enclosing_definition` on its code.
2. Stack traces: if its traces have a new format, add a module in `codeintel/stacktrace/` with
   `detect(text)` and `parse(text)`, a member of `Family` in `family.py`, and an entry in
   `_PARSERS`. If its frames name files in another form than the repository does, teach
   `codeintel/paths.py` that form.
3. Fixtures: a sample app with a known bug under `fixtures/apps/<language>/`, an incident under
   `fixtures/incidents/<id>/` with a crash log in the language's real trace format, the
   expected result, and the recorded reply (`fixtures/FORMAT.md`). Add `<language>:<id>` to
   `INCIDENTS` in the Makefile, so `make incident LANG=<language>` sends it.
4. Run `make test`: the fixture checks, the stack-trace tests and the evals cover it.

## Use the persistence and tracing in your own process

The worker is one host for a graph. Your own process, such as a web app, a scheduled job or a
script, can run a graph with the same persistence and tracing: the redacting LangSmith client
(P15), the pool (P1, P2), the strict checkpoint serializer with its optional `LANGGRAPH_AES_KEY`
encryption (P7), the pool-aware checkpointer (P3) and the Store (P24). `run_once` below makes the
worker's startup calls (`Worker._start` in `worker/main.py`) in the same order, and invokes the
graph as the run executor does: only `thread_id` in `configurable`, the run's fixed inputs in the
context (P14), and `durability="sync"`, so each step's checkpoint is written before the next step
starts (P11). It imports nothing from `worker/`, so it still works after you delete the worker.
`tests/integration/test_host_recipe.py` runs this block, so it stays true.

- Apply the migrations first (`migrate`, or `run_migrate` from your entry point): they create the
  checkpoint and Store tables. The recipe needs none of the worker's tables.
- `thread_id` is a UUID string, such as `str(uuid.uuid7())`. It is also the run's ID in the
  context, as in the worker.
- Input that fails validation raises pydantic's `ValidationError`, before anything opens. Its
  message shows the input's values, which can hold secrets: log `exc.errors(include_input=False)`.
- A graph that stops at an interrupt returns normally, with an `__interrupt__` key in the result:
  the run has not ended. The worker counts it as a failed attempt.
- `run_once` opens a pool and the tracing client, and closes them, on each call. A long-lived
  process does the setup once at startup, keeps the pool, the saver, the store and the compiled
  graph, invokes the graph per request, and closes the pool and the tracing client at shutdown.
- The block names the package; `scripts/rename.py` renames it here too.

<!-- host-recipe:begin -->
```python
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from langgraph_and_observability.agent_spec import AgentSpec, RunInfo
from langgraph_and_observability.observability.langsmith import close_tracing, configure_tracing
from langgraph_and_observability.persistence.checkpointer import make_checkpointer
from langgraph_and_observability.persistence.pool import create_pool, open_pool
from langgraph_and_observability.persistence.resources import set_pool
from langgraph_and_observability.persistence.serde import make_serde
from langgraph_and_observability.persistence.store import make_store
from langgraph_and_observability.settings import Settings


async def run_once(
    spec: AgentSpec, payload: Mapping[str, Any], *, settings: Settings, thread_id: str
) -> dict[str, Any]:
    """Run the graph of ``spec`` once on ``payload`` and return its final state."""
    model = spec.input_model.model_validate(payload)
    tracing = configure_tracing(settings)
    try:
        pool = create_pool(settings)
        # When Postgres can't be reached in time, open_pool closes the pool and raises.
        await open_pool(pool, timeout=settings.db_connect_timeout_s)
        # For nodes that use the pool directly (persistence.resources.current_pool).
        set_pool(pool)
        try:
            aes_key = settings.langgraph_aes_key
            serde = make_serde(
                spec.serde_types,
                aes_key=None if aes_key is None else aes_key.get_secret_value(),
            )
            saver = make_checkpointer(pool, serde, pgbouncer_mode=settings.pgbouncer_mode)
            store = make_store(pool, pgbouncer_mode=settings.pgbouncer_mode)
            graph = spec.build_graph().compile(checkpointer=saver, store=store)
            run = RunInfo(
                run_id=UUID(thread_id),
                dedup_key=spec.dedup_key(model, datetime.now(UTC)),
                attempt=1,
            )
            return await graph.ainvoke(
                spec.initial_state(model),
                {"configurable": {"thread_id": thread_id}},
                context=spec.make_context(run, model, settings),
                durability="sync",
            )
        finally:
            set_pool(None)
            await pool.close()
    finally:
        if tracing is not None:
            # Sends the pending traces.
            await close_tracing(tracing)
```
<!-- host-recipe:end -->

`langgraph dev` and LangSmith Deployment (the Agent Server) do not run this code: on their path
they bring their own checkpointer and store, so the graph gets neither this template's strict
serializer, nor its `LANGGRAPH_AES_KEY` encryption, nor the pool-aware saver.

## Run without the queue worker

A fork that only wants Studio, or another trigger, can delete the worker (spec §13.9): nothing
else in the package imports it. Delete these files and folders:

- `src/<package>/worker/`
- `tests/unit/test_executor_logs.py`
- `tests/unit/test_health.py`
- `tests/unit/test_intake_invalid.py`
- `tests/unit/test_registry.py`
- `tests/unit/test_worker_main.py`
- `tests/integration/test_cli.py`
- `tests/integration/test_executor.py`
- `tests/integration/test_intake.py`
- `tests/integration/test_retention.py`
- `tests/integration/test_runs_repo.py`
- `tests/integration/test_worker_loop.py`
- `tests/e2e/`

`make lint typecheck test` then passes: the docs tests skip the practice notes' paths that went
with the worker, and `scripts/remove_example.py` still removes the example. What the worker
gave, and what to change:

- **The commands.** `worker`, `migrate` and `runs` live in `worker/cli.py`. Without it,
  `python -m <package>` prints its version, and a command exits with an error. Call the
  migration runner from your own entry point:

  ```python
  import asyncio

  from <package>.persistence.migrate import run_migrate
  from <package>.settings import load_settings

  asyncio.run(run_migrate(load_settings(), agent_sources=[("my_agent", MIGRATIONS_DIR)]))
  ```

- **Compose and the image.** Delete the `worker` and `elasticmq` services of `compose.yaml`, and
  point its `migrate` service, and the Dockerfile's `CMD`, at your entry point. `make up` and
  `make migrate` use them.
- **CI.** The `e2e` job then finds no tests: delete it from `.github/workflows/ci.yml`, with the
  Makefile's `test-e2e` target and the `e2e` lines of
  `tests/unit/test_ci_config.py::test_test_jobs_run_their_make_targets`. The `docker` job's
  migration smoke test runs the image's `migrate` command: point it at your entry point.
- **The deployment.** `deploy/k8s/` runs the worker and the `migrate` command; replace them for
  your trigger.
- **Retention** (`worker/retention.py`, P6): prune old checkpoints some other way, such as a
  scheduled job that calls `saver.aprune(thread_ids, strategy="delete")`.
- **The registry** (`worker/registry.py`): `langgraph.json` names graphs directly, so Studio
  does not need it.
