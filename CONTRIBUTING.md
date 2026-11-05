# Contributing

Thank you for helping. This project is a template: plain LangGraph and Python that people fork and
change. Read [AGENTS.md](AGENTS.md) for the rules that every change follows.

## Set up

1. Install [uv](https://docs.astral.sh/uv/). uv installs Python 3.14 when it is missing.
2. Install the project and every dependency group:

   ```bash
   make setup
   ```

3. Install the git hooks (ruff, pylint, mypy, gitleaks, the lockfile check and basic hygiene):

   ```bash
   uv run pre-commit install
   ```

## Before you open a pull request

- Run `make lint typecheck test`. All three must pass.
- Run `uv run pytest -m integration` if your change touches Postgres or the queue (needs Docker).
- Add or update tests. Warnings are errors; see AGENTS.md before you add a warning filter.
- A new practice needs a test and a note in `docs/practices/`.
- Add a line to [CHANGELOG.md](CHANGELOG.md) under "Unreleased" for a user-facing change.

## Dependencies

Add a dependency with uv and commit the updated `uv.lock`:

```bash
uv add <package>                 # runtime
uv add --group test <package>    # test-only
```

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/) for commit subjects, for example
`feat: add Datadog evidence step` or `fix: release the lease on drain`.

## Issues and security

Use the issue forms for bugs and feature requests. Do not report security problems in public;
follow [SECURITY.md](SECURITY.md).

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By taking part, you agree to it.
