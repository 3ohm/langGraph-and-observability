# Customize

A fork usually does three things: renames the package, removes or keeps the example agent, and
adds its own agent. This guide also shows how to add an evidence collector or a language to the
example, and how to run without the queue worker. The rules that every change follows are in
[AGENTS.md](../../AGENTS.md).

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

## Remove the example agent

```bash
uv run scripts/remove_example.py
make lint typecheck test
```

It deletes `src/<package>/agents/incident_triage/` and the files that only the example uses
(`scripts/send_incident.py`, `scripts/sync_dataset.py`, their tests, the agent's end-to-end test,
`.github/workflows/evals.yml` and `docs/guides/real-mode.md`, with its links in `README.md` and
the other docs). It
removes the agent's `langgraph.json` entry and registry line, resets the settings defaults that
name it (`APP_INTAKE_AGENT` becomes empty), and deletes the example's parts of the Makefile
(`grammars`, `incident`), the Dockerfile (the tree-sitter grammars) and CI, which those files mark
with `# example-agent:begin` and `# example-agent:end` lines. It prints each change; a second run
changes nothing. On a first run it refuses, and changes nothing, when a file it must edit has
drifted from the text it expects, or when the removal would leave a Makefile recipe or CI step
calling a make target that it deletes. CI's `removal` job proves on every commit that the rest
still passes (P22).

Then:

- update `README.md`, `AGENTS.md` and `docs/`, which still describe the example;
- trim what only the example needs: the Datadog, Git and ECR settings, the read-only ClusterRole
  in `deploy/k8s/rbac.yaml`, and the Bedrock and ECR statements of `deploy/iam/policy.json`;
- register your agent and set `APP_INTAKE_AGENT`: without an agent, a worker with
  `APP_SQS_QUEUE_URL` set refuses to start.

## Add an agent

An agent is a plain LangGraph `StateGraph` plus an `AgentSpec` (`agent_spec.py`): the functions
that the worker calls around the graph.

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
     module level, P7), and optionally `migrations_dir` and `retention_sql` (P6).
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

### The dedup key

The worker maps messages with one dedup key to one run (P8). The example's key is
`sha256(cluster | namespace | workload or pod | alert_id, or else the 15-minute bucket of
fired_at)`. With an `alert_id`, time is ignored: two messages with the same `alert_id` are one
run, however far apart. So the `alert_id` must identify one occurrence of an alert, such as the
alert event's ID, never a stable monitor ID; otherwise every later incident from that monitor is
dropped as a duplicate of the first. Your agent's `dedup_key` needs the same care.

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
