# AGENTS.md

Rules for coding assistants and people who work in this repository. `CLAUDE.md` imports this file.

## What this repository is

A template, not a framework (see `docs/adr/0001-template-not-framework.md`). It is plain LangGraph
and Python, with no wrapper layer. People fork it, delete the parts they don't need, and build their
own agent. The design is in `docs/design/`; the implementation plans are in `docs/plans/`.

## Project map

v0.1.0 is under construction, so some paths below do not exist yet.

```
src/langgraph_postgres_starter/
  __main__.py               CLI: python -m langgraph_postgres_starter
  settings.py logging.py ids.py redaction.py policies.py agent_spec.py
  persistence/              connection pool, migrations, serializer, checkpointer, action ledger
  worker/                   queue worker: intake, runs, executor, health, CLI, retention
  observability/            LangSmith tracing and run metadata
  agents/incident_triage/   the example agent: graph, nodes, adapters, codeintel, fixtures, evals, tests
tests/                      unit/  integration/  e2e/  graphs/  conftest.py
docs/                       design/  plans/  adr/  practices/  guides/
deploy/                     example manifests (Kubernetes, IAM, SQL, ElasticMQ)
scripts/                    rename, remove the example agent, send an incident, sync the eval dataset
.claude/agents/             build agents (implementer, reviewer)
conftest.py                 fixtures for every test directory (environment isolation, strict msgpack)
```

## Commands

Use uv for everything (`uv add`, `uv run`). Do not use pip. Commit `uv.lock`.

```bash
make setup       # uv sync --all-groups
make lint        # ruff check + ruff format --check
make fmt         # ruff fixes, then ruff format
make typecheck   # mypy (strict) on src, tests and conftest.py
make test        # unit tests with coverage: no Docker, no network, no credentials
uv run pytest -m integration   # needs Docker
uv run pytest -m e2e           # starts worker subprocesses
```

## Rules

1. **Adapters.** Every external system is reached through an adapter interface (a `Protocol`) that
   has a fake. Import vendor SDKs (`kubernetes_asyncio`, `datadog_api_client`, `boto3`/`botocore`,
   `langsmith`) only under `agents/*/adapters/`, `worker/intake.py`, `observability/` and
   `policies.py` (the last for exception types only).
2. **Async and idempotent.** Every graph step is `async`, and every side effect is idempotent.
3. **No secrets** in graph state, in `configurable`, in logs, in traces or in prompts. All evidence
   text passes `redaction.redact()` before it enters state. `configurable` holds only `thread_id`.
4. **Evidence IDs.** Evidence items have IDs, and results cite only those IDs.
5. **Practices.** Every new practice needs a test and a note in `docs/practices/`.
6. **Plain LangGraph.** Do not add helpers that hide LangGraph's APIs.

## Conventions

- Python 3.14. Do not add `from __future__ import annotations`. Use `uuid.uuid7()` for time-ordered IDs.
- mypy strict for package code. Test modules may leave functions unannotated; mypy still checks them.
- Warnings are errors in tests. Add a filter only for an unavoidable third-party warning: scope it
  to the message and the module, and add a comment that says why.
- Settings use the `APP_` prefix. Vendor variables keep their own names: `DD_*`, `LANGSMITH_*`,
  `LANGGRAPH_*`, `AWS_*`.
- Unit tests use no Docker, no network and no credentials. Mark Docker tests `integration` and
  worker-subprocess tests `e2e`.
- Commit subjects follow Conventional Commits.

## Extending

Guides for adding an evidence collector, a language or an agent come with the example agent in v0.1.0.
