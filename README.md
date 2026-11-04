# langgraph-postgres-starter

A template for durable LangGraph agents on Postgres. It is built so that a run
survives a crash or a restart and continues from its last checkpoint, with LangSmith
tracing built in. The example agent is incident triage on EKS.

> **Status:** under construction (`0.1.0.dev0`). This README is a stub. The full
> quickstart, the "real mode on EKS" guide and the practices list come with v0.1.0.

## Development

You need [uv](https://docs.astral.sh/uv/) and Python 3.14.

```bash
make setup      # install all dependency groups
make lint       # ruff lint and format check
make typecheck  # mypy (strict)
make test       # unit tests with coverage
```

See [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md).

## License

[MIT](LICENSE)
