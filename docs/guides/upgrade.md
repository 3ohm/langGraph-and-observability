# Upgrade

This guide covers dependency and Python upgrades, schema changes in your own fork, rolling out a
new version, and taking later changes from the template. The template is meant to be copied, not
installed (`docs/adr/0001-template-not-framework.md`), so a fork owns its code and upgrades on
its own schedule.

## Dependencies

- **Dependabot** opens weekly pull requests for Python packages, GitHub Actions, the
  Dockerfile's images and the Compose images, with the LangChain-family packages
  (`langgraph*`, `langchain*`, `langsmith`) in one group and a 7-day cooldown, so a broken
  release has time to be pulled. Actions stay pinned to full commit SHAs; Dependabot updates the
  SHA and its tag comment. CI's service containers use the Compose images, but Dependabot does
  not update them: change them in `.github/workflows/ci.yml` in the same pull request.
- **By hand:** `uv lock --upgrade-package <name>` (or `uv lock --upgrade` for all), then
  `uv sync --all-groups` and commit `uv.lock`. The pre-commit hook `uv lock --check` fails when
  `uv.lock` does not match `pyproject.toml`, and so does every uv command in CI (`UV_LOCKED`).
- Run the whole suite after an upgrade: `make lint typecheck test`, then `make test-int` and
  `make test-e2e` (Docker). The evals run in `make test` with the recorded model; run the Evals
  workflow for a prompt or model change.

## LangGraph and the checkpointer

- **The pool-aware saver (P3)** copies a private method of `AsyncPostgresSaver`. When an upgrade
  changes it, `tests/unit/test_upstream_shape.py` fails: compare the new upstream method with
  the copy in `persistence/checkpointer.py` and update the copy. Once upstream stops locking on
  a pool, delete the subclass and use `AsyncPostgresSaver` directly.
- **Checkpoint and Store migrations:** `migrate` runs `AsyncPostgresSaver.setup()` and
  `AsyncPostgresStore.setup()` every time, and each `setup()` applies the new checkpoint or Store
  migrations of a library upgrade. So run the migrate Job (or `make migrate`) before you roll
  out the new image.
- **Serialization (P7):** the allowlist is explicit, so an upgrade does not widen it. A new state
  type still has to be added to the agent's `serde_types`; the save-and-reload tests show a
  missing one.
- **Retry and timeout policies (P12):** recheck `policies.is_transient`, and each agent's own
  predicate (the example's `agents/incident_triage/policies.py`), against the release notes
  when LangGraph or a vendor client changes its errors or its own retries.

## Python

The project uses Python 3.14 (`.python-version`, with `requires-python = ">=3.14"`). uv installs
it, locally and in every CI job (`astral-sh/setup-uv`). To move up: change `.python-version`, `requires-python`,
ruff's `target-version`, pylint's `py-version` and mypy's `python_version` in `pyproject.toml`,
the `python-version` of the CI jobs and the `python:3.14-slim` base of the Dockerfile, then run
`uv lock` and the whole suite.

## Your own schema

Migrations are forward-only. Never edit a migration after it is released; add the next file,
`NNNN_<name>.sql`, to the component's folder. A statement such as `CREATE INDEX CONCURRENTLY`
goes in a file of its own that starts with `-- migrate:no-transaction`, written so that it is
safe to repeat (`IF NOT EXISTS`). Keep each change compatible with the image that is still
running during a rollout: add a column first, use it in the next release, remove the old one
after that.

## Roll out a new version

1. Build and push the image.
2. Run the migrations first. On Kubernetes, delete the finished migrate Job, apply everything
   except the worker, and wait for the Job ([run-on-eks.md](run-on-eks.md#5-apply-the-migrate-job-first)).
   Locally, `make migrate`.
3. Apply the Deployment. Each old pod drains on SIGTERM within 45 s and returns its runs to
   pending; the new pods resume them at their last step (P10, P11).

## Take changes from the template

The template keeps a changelog (`CHANGELOG.md`) with SemVer tags. To port a release into a fork,
read its entries, then apply the template's diff between two tags with your names:

```bash
git remote add template https://github.com/3ohm/langGraph-and-observability.git
git fetch template --tags
git diff v0.1.0 v0.2.0 \
  | sed -e 's/langgraph[_]and[_]observability/langgraph_and_observability/g' \
        -e 's/langgraph[-]and[-]observability/langgraph-and-observability/g' \
  | git apply --reject
```

Each `sed` expression turns one of the template's names into yours. `scripts/rename.py` writes
your names on the right side when it renames the package. The brackets on the left match the
same characters but stop the script from rewriting the template's names, so the command still
works after a rename. If you renamed the package by hand, put your names on the right side.

`--reject` applies what fits and leaves a `.rej` file for each hunk to resolve by hand. Changes
to the example agent do not apply to a fork that removed it; skip them.
