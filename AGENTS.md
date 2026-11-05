# AGENTS.md

Rules for coding assistants and people who work in this repository. `CLAUDE.md` imports this file.

## What this repository is

A template, not a framework (see `docs/adr/0001-template-not-framework.md`). It is plain LangGraph
and Python, with no wrapper layer. People fork it, delete the parts they don't need, and build their
own agent. The design is in `docs/design/`; the implementation plans are in `docs/plans/`.

## Project map

```
src/langgraph_and_observability/
  __main__.py               CLI: python -m langgraph_and_observability (worker, migrate, runs)
  settings.py logging.py ids.py redaction.py policies.py agent_spec.py
  persistence/              connection pool, migrations, serializer, checkpointer, store, action ledger
  worker/                   queue worker: intake, runs, executor, health, CLI, registry, retention
  observability/            LangSmith tracing and run metadata
  agents/incident_triage/   the example agent: graph, nodes, adapters, codeintel, fixtures, evals, tests
tests/                      unit/  integration/  e2e/  graphs/ (the test-only steps graph)  conftest.py
docs/                       design/  plans/  adr/  practices/ (P1-P24)  guides/
deploy/                     example manifests (Kubernetes, IAM, SQL roles, ElasticMQ)
scripts/                    rename.py  remove_example.py  send_incident.py  sync_dataset.py
.claude/agents/             build agents (implementer, reviewer)
conftest.py                 fixtures for every test directory (environment isolation, strict msgpack,
                            Postgres from tests/fixtures_pg.py)
langgraph.json              the graphs that Studio serves (make studio)
```

## Commands

Use uv for everything (`uv add`, `uv run`). Do not use pip. Commit `uv.lock`.

```bash
make setup       # uv sync --all-groups, then make grammars
make grammars    # fetch the tree-sitter grammars (APP_GRAMMAR_DIR or the user cache)
make lint        # ruff check + ruff format --check, then make pylint
make pylint      # pylint on package code and scripts, then on the tests with fewer checks
make fmt         # ruff fixes, then ruff format
make typecheck   # mypy (strict) on src, tests, scripts and conftest.py
make test        # unit tests with coverage: no Docker, no network, no credentials
make test-int    # integration tests (uv run pytest -m integration): needs Docker
make test-e2e    # end-to-end tests (uv run pytest -m e2e): worker subprocesses, needs Docker
make up          # the local stack: postgres, elasticmq, migrate, worker (docker compose)
make incident LANG=python   # send a fixture incident to the local stack (or FIXTURE=<id>)
make migrate     # apply the migrations to the local stack's Postgres
make studio      # langgraph dev: the graphs of langgraph.json in LangGraph Studio
make down        # stop the local stack (its database volume stays)
uv run scripts/rename.py <new_package_name> [--dry-run]   # rename the package for a fork
uv run scripts/remove_example.py                          # remove the example agent
```

## Rules

1. **Adapters.** Every external system is reached through an adapter interface (a `Protocol`) that
   has a fake. Import vendor SDKs (`kubernetes_asyncio`, `datadog_api_client`, `boto3`/`botocore`,
   `langsmith`) only under `agents/*/adapters/`, `worker/intake.py`, `observability/`,
   `agents/*/policies.py` (the last for exception types only) and `scripts/` (operator scripts,
   such as `send_incident.py` and `sync_dataset.py`, which the package never imports).
2. **Async and idempotent.** Every graph step is `async`, and every side effect is idempotent.
3. **No secrets** in graph state, in `configurable`, in logs, in traces or in prompts. All evidence
   text passes `redaction.redact()` before it enters state. `configurable` holds only `thread_id`.
4. **Evidence IDs.** Evidence items have IDs, and results cite only those IDs.
5. **Practices.** Every new practice needs a test and a note in `docs/practices/`, with the
   sections Problem, What the template does, Where and Proof, in at most 40 lines.
6. **Plain LangGraph.** Do not add helpers that hide LangGraph's APIs.
7. **Example-only parts.** A part of a shared file that only the example agent uses sits between
   `# example-agent:begin` and `# example-agent:end` lines, so `scripts/remove_example.py` can
   delete it. Keep `remove_example.py` in step when you add one or change a text it edits:
   on a first run it refuses a drifted file, and CI's `removal` job checks the result.

## Conventions

- Python 3.14. Do not add `from __future__ import annotations`. Use `uuid.uuid7()` for time-ordered IDs.
- mypy strict for package code. Test modules may leave functions unannotated; mypy still checks them.
- ruff lints and is the only formatter (do not add black). pylint lints on top of ruff, with its
  config in `pyproject.toml`. Fix a pylint message before you silence it; silence one line with
  `# pylint: disable=<symbolic-name>  # reason`, never with a message code. Test modules skip
  the checks in the Makefile's `PYLINT_TEST_DISABLE`.
- Warnings are errors in tests. Add a filter only for an unavoidable third-party warning: scope it
  to the message and the module, and add a comment that says why.
- Settings use the `APP_` prefix. Vendor variables keep their own names: `DD_*`, `LANGSMITH_*`,
  `LANGGRAPH_*`, `AWS_*`. `.env.example` documents every setting with its default.
- Unit tests use no Docker, no network and no credentials. Mark Docker tests `integration` and
  worker-subprocess tests `e2e`.
- Commit subjects follow Conventional Commits.

## How to add an agent

1. Make `src/langgraph_and_observability/agents/<name>/` with `graph.py` (`build_graph()` returns
   the uncompiled `StateGraph`; every node is `async`, with `default_retry_policy()` and
   `step_timeout(...)` from `policies.py`), its state, context and schemas, and `spec.py` with
   one `AgentSpec`: name, graph, input model, dedup key, context, initial state, `serde_types`,
   and optional `migrations_dir` and `retention_sql`.
2. Put each external system behind `adapters/<system>/interface.py` (a `Protocol`), with a real
   adapter and a fake. Put the agent's tests in `agents/<name>/tests/`.
3. Register it: one line in `builtin_agents()` of `worker/registry.py` (or an `APP_EXTRA_AGENTS`
   entry), one entry in `langgraph.json`, and `APP_INTAKE_AGENT=<name>` for queue intake.
4. Details, and the dedup `alert_id` rule: `docs/guides/customize.md`.

## How to add an evidence collector (to the example agent)

1. An adapter under `agents/incident_triage/adapters/<system>/`: interface, real, fake; add it to
   `Adapters` and `build_adapters`, and give each incident fixture its data.
2. An `async` step in `nodes/<system>.py` that returns a capped list of items built with
   `evidence.make_item` (it redacts and caps) and IDs from `evidence.make_id`; add the kind and
   the ID prefix in `schemas.py`.
3. Wire it in `graph.py` with a timeout (`TIMEOUTS`) and, if the run can go on without it, an
   error handler that adds a note.
4. Add a section in `prompts.render_user_message`, the state key to the collected IDs in
   `nodes/analyze.py` and `evals/evaluators.py`, bump `PROMPT_VERSION`, and record the replies.

## How to add a language (to the example agent)

1. A `LanguageSpec` in `codeintel/languages.py` (extensions, definition node types); then
   `make grammars`.
2. If its stack traces have a new format: a parser module in `codeintel/stacktrace/` (`detect`,
   `parse`), a `Family` member, and an entry in `_PARSERS`. Teach `codeintel/paths.py` any new
   file-name form.
3. A sample app, an incident fixture, the expected result and the recorded reply
   (`fixtures/FORMAT.md`), and a `<language>:<id>` entry in the Makefile's `INCIDENTS`.
