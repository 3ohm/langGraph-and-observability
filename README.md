# langGraph-and-observability

A template for LangGraph agents that hold up in production, with Postgres persistence and
LangSmith built in, on Python 3.14. A run survives a crash, a restart or a redeploy, and goes on
from its last saved step on another pod. Every production practice in it is real code, with a
test and a short note that explains why.

It ships with a working agent, **Incident Triage**. A message on Amazon SQS names a crashing
Kubernetes workload. The agent collects Kubernetes and Datadog evidence, reads the crashing code
at the deployed commit when the evidence points at the app, and asks Claude on Amazon Bedrock
for a typed triage that cites its evidence. It only reads. Fork the repository, keep or remove
the example, and build your own agent on the same base.

> **Status:** `0.1.0.dev0`, before the first release. The Python package is
> `langgraph_postgres_starter`; the distribution, the image and the Kubernetes names use
> `langgraph-postgres-starter`.

## What's inside

- **Postgres persistence:** one health-checked connection pool per process, a pool-aware
  checkpointer, strict serialization with optional encryption, plain-SQL migrations as their own
  step, two database roles, and retention for checkpoints and results.
- **A queue worker:** SQS intake with dedup and a dead-letter queue, leases and heartbeats,
  periodic recovery, a drain within Kubernetes' 60-second shutdown hook, idempotent publishing,
  health endpoints and an operator CLI.
- **Plain LangGraph:** `durability="sync"`, explicit retry, timeout and error-handler policies, a
  `context_schema`, and no wrapper layer (`docs/adr/0001-template-not-framework.md`).
- **LangSmith:** one redacting client, trace content modes, run metadata, and evals in CI.
- **The example agent:** twelve sample apps in eleven languages with real crash traces,
  tree-sitter code extraction, and recorded model replies, so it all runs without credentials.
- **Deployment:** a Dockerfile, a Compose stack, and EKS manifests with read-only RBAC and an
  IAM policy.
- **Tooling:** uv, ruff, mypy (strict), pytest with warnings as errors, and CI with a removal
  job and gated evals.

## Quickstart

No credentials: Docker and [uv](https://docs.astral.sh/uv/) are enough. Clone the repository
(`git clone https://github.com/3ohm/langGraph-and-observability.git`), then run these commands
in it:

```bash
make up                    # build the image; start Postgres, ElasticMQ, migrate, then the worker
make incident LANG=python  # send the Python sample app's crash to the queue
docker compose logs worker # the result is the triage.published line
```

The worker runs in simulated mode: fake Kubernetes and Datadog adapters that serve fixtures, the
sample app's source from a local folder, and recorded model replies. The
[quickstart guide](docs/guides/quickstart.md) lists the other incidents, shows how to look at
runs and results, and starts LangGraph Studio (`make studio`).

## Real mode on EKS

- [docs/guides/run-on-eks.md](docs/guides/run-on-eks.md): the image, the database roles, the
  Secrets, IAM and the model ID, the apply order (migrations first), PgBouncer, code evidence,
  and how to debug a stuck run with `python -m asyncio ps`.
- [docs/guides/real-mode.md](docs/guides/real-mode.md): eight steps to a first real triage, each
  real adapter (Kubernetes, Datadog, Bedrock, Git and ECR, SQS) and its settings.
- [deploy/](deploy/): the example manifests, the IAM policy and the database roles script.

## Make it yours

- [docs/guides/customize.md](docs/guides/customize.md): rename the package
  (`scripts/rename.py`), remove the example agent (`scripts/remove_example.py`), and add an
  agent, an evidence collector or a language.
- [docs/guides/upgrade.md](docs/guides/upgrade.md): dependencies, LangGraph, Python, schema
  changes and rollouts.
- [AGENTS.md](AGENTS.md): the rules of the repository, for people and for coding assistants.

## Practices

Each practice ships as code, a test and a note. The core ones (P8 to P13) are proven on a
test-only graph, so they keep their proof when you remove the example agent.

| # | Practice |
|---|---|
| [P1](docs/practices/P1.md) | Pooled, health-checked connections; startup fails without a database |
| [P2](docs/practices/P2.md) | PgBouncer-safe mode |
| [P3](docs/practices/P3.md) | Pool-aware checkpointer |
| [P4](docs/practices/P4.md) | Migrations as a separate step; minimal database roles |
| [P5](docs/practices/P5.md) | Readiness that checks the database |
| [P6](docs/practices/P6.md) | Retention for checkpoints and results, run by one leader |
| [P7](docs/practices/P7.md) | Strict serialization with an allowlist, optional encryption |
| [P8](docs/practices/P8.md) | Queue intake: validate, dedup, record, then delete; bad messages go to the DLQ |
| [P9](docs/practices/P9.md) | Leases, heartbeats, periodic recovery |
| [P10](docs/practices/P10.md) | Drain within the 60-second hook |
| [P11](docs/practices/P11.md) | Resume at the failed step |
| [P12](docs/practices/P12.md) | Explicit retry, timeout and error-handler defaults |
| [P13](docs/practices/P13.md) | Idempotent publishing |
| [P14](docs/practices/P14.md) | No secrets or identity in `configurable`; a `context_schema` |
| [P15](docs/practices/P15.md) | Redaction before the LLM and in traces; trace content modes |
| [P16](docs/practices/P16.md) | Typed structured output with validated citations and one repair retry |
| [P17](docs/practices/P17.md) | Bounded evidence (caps and the code budget) |
| [P18](docs/practices/P18.md) | Provider constraints: no sampling parameters, no forced tool choice, refusals handled |
| [P19](docs/practices/P19.md) | Read-only, least-privilege access |
| [P20](docs/practices/P20.md) | Deterministic, table-tested assessment rules |
| [P21](docs/practices/P21.md) | Evals gate pull requests |
| [P22](docs/practices/P22.md) | Removable example agent |
| [P23](docs/practices/P23.md) | Cache-friendly prompts |

## Development

You need uv; it installs Python 3.14 when it is missing.

```bash
make setup      # install every dependency group, then fetch the tree-sitter grammars
make lint       # ruff lint and format check
make typecheck  # mypy (strict)
make test       # unit tests with coverage: no Docker, no network, no credentials
make test-int   # integration tests against Postgres and ElasticMQ (needs Docker)
make test-e2e   # worker subprocesses, kill -9 and SIGTERM (needs Docker)
```

The design is in [docs/design/](docs/design/2026-11-01-langgraph-postgres-starter-design.md).
See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute and [SECURITY.md](SECURITY.md) to report a
security problem.

## License

[MIT](LICENSE)
