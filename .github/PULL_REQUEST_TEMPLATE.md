## What and why

<!-- What does this change, and why? Link the issue, for example "Closes #123". -->

## How it was tested

<!-- The commands you ran and their result. -->

## Checklist

- [ ] `make lint typecheck test` passes.
- [ ] Tests are added or updated (`uv run pytest -m integration` for Postgres or queue changes).
- [ ] A new practice has a test and a note in `docs/practices/`.
- [ ] No secrets in graph state, `configurable`, logs, traces or prompts.
- [ ] `CHANGELOG.md` has a line under "Unreleased" for a user-facing change.
- [ ] Commit subjects follow Conventional Commits.
