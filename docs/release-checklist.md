# Release checklist: v0.1.0

Phase 1 ends with a tagged v0.1.0 and a manual smoke test in real mode on the owner's AWS account
(spec §14). Do the steps in order. Steps 1 to 3 test a release candidate, a commit on the branch
you release from; step 4 adds the release commit on top of it, and step 5 tags that commit once
CI is green on it. Tag only after the smoke test has passed.

## 1. CI is green

- On GitHub, every job of the CI workflow (`.github/workflows/ci.yml`) passes on the release
  candidate: `lint`, `unit`, `integration`, `e2e`, `docker` and `removal`. The `removal` job runs
  `scripts/remove_example.py` on a copy of the commit, then the unit tests, lint and typecheck
  without the example agent.
- Locally, from a clean checkout of the same commit, with Docker running:

  ```bash
  uv lock --check
  make lint typecheck test test-int test-e2e
  ```

## 2. Live evals

Normal CI runs the evals with recorded model replies. Run them once with the real model (the
Evals workflow, `.github/workflows/evals.yml`):

1. Set the repository variable `AWS_EVALS_ROLE_ARN` (an IAM role that trusts this repository's
   GitHub OIDC tokens and allows `bedrock:InvokeModel`), the variable `AWS_REGION`, and
   `BEDROCK_MODEL_ID` where the bare model ID does not run in that Region (for example
   `us.anthropic.claude-opus-5-5`). `JUDGE_MODEL_ID` is optional. Add the secret
   `LANGSMITH_API_KEY` (and the variable `LANGSMITH_ENDPOINT` outside LangSmith's US region).
   The role's trust policy needs `token.actions.githubusercontent.com:aud` equal to
   `sts.amazonaws.com`, and `token.actions.githubusercontent.com:sub` matched with `StringLike`
   against `repo:3ohm/langGraph-and-observability:*` (pull requests and manual runs). The
   claim keeps the repository name's capital letter, and IAM compares it case-sensitively.
2. Run the workflow: Actions, Evals, Run workflow (or `gh workflow run evals.yml --ref <branch>`).
3. Check the scorecard in the job summary: `category_match` at least 0.9, `code_location_hit` at
   least 0.8 and `citations_valid` 1.0. The root-cause judge's score is reported, with no
   threshold.
4. Open the LangSmith experiment from the scorecard's link.
5. Look at the `java-spring-npe` case with the real model. Its log is a long Spring trace, and
   the log evidence keeps only the last 4000 characters, so the exception's header line can be
   cut off; note whether the real model still finds the cause.

## 3. Real-mode smoke test

Follow [guides/real-mode.md](guides/real-mode.md), steps 1 to 8, with the owner's AWS, EKS,
Datadog and Git access, on one real crashed pod. It passes when the run gives a valid
`TriageResult` that passes the checks of step 8: in `triage_results`, in the `triage.published`
log line and, with the `sqs` sink, on the result queue.

Write down, without secrets or account IDs: the date, the Region, the model ID or inference
profile, the kind of crash (language, or infrastructure cause), the result's `category`,
`confidence` and `needs_human`, whether `suspected_code` pointed at the right lines, and anything
you had to fix on the way.

## 4. CHANGELOG and version

1. In `CHANGELOG.md`, change the `0.1.0` heading's "pending owner smoke test" to the release date,
   `## [0.1.0] - YYYY-MM-DD`, and add the smoke test's date and result from step 3 under it. Keep
   `## [Unreleased]` above it, empty.
2. Set the version to `0.1.0`: `version` in `pyproject.toml`, then `uv lock` (the lock records
   the project's version), the expected version in `tests/unit/test_package.py`, and the status
   line in `README.md`.
3. Run `make lint typecheck test`, and commit: `chore: release 0.1.0`.

## 5. Tag

After CI is green on the release commit:

```bash
git tag -a v0.1.0 -m "v0.1.0"
git push origin v0.1.0
```

Then create a GitHub release from the tag, with the `0.1.0` section of `CHANGELOG.md` as its
notes.

## Owner actions before launch

These are open items from the build that need the owner's accounts, an outward-facing action, or
a check that the builders could not run.

- **O1, project contacts.** Replace `[INSERT CONTACT METHOD]` in `CODE_OF_CONDUCT.md` with a
  real contact. GitHub's private vulnerability reporting, which `SECURITY.md` tells reporters to
  use, is on since 2026-11-05.
- **O2, the first CI run on GitHub.** Watch the first complete run: every job on a real runner,
  including the amd64 `docker` job with its Compose migration smoke test and the `integration`
  and `e2e` jobs, and the runner image (the jobs install ripgrep, which `ubuntu-latest` lacks).
- **O3, the upstream checkpointer issue.** Open the issue and pull request in LangGraph's
  repository for the `AsyncPostgresSaver` `_cursor` lock that spec §7.2 describes, then add
  their links to the docstring of `src/langgraph_and_observability/persistence/checkpointer.py`.
- **O4, the producer's `alert_id`.** Confirm what the system that sends incidents puts in
  `alert_id`. It must identify one occurrence of an alert, not a stable monitor ID: with an
  `alert_id`, dedup ignores time, so a reused ID drops every later incident as a duplicate.
- **O5, Git hosts and tokens.** Accept that private GitLab repositories are not supported in this
  version (GitLab's git HTTP endpoint ignores a Bearer token). Check once, with a valid read-only
  token, that GitHub accepts it as `Authorization: Bearer` for a git fetch over HTTPS: only
  invalid tokens were tried.
- **O6, the tree-sitter bug.** Report upstream that in tree-sitter 0.26.0, reading `Point.row` or
  `Point.column` for rows above 256 can free the integer and crash the process. The code works
  around it by indexing points (`point[0]`, `point[1]`).
- **O7, the model ID and Bedrock claims.** Confirm the model ID or inference profile in your
  account (`aws bedrock list-inference-profiles`), and that the three Bedrock statements of
  `deploy/iam/policy.json` match it. Spot-check the AWS-only claims in `docs/practices/P16.md`:
  the bare model ID runs in-Region only in `eu-west-2`; structured output allows at most 12
  optional and 16 union-typed properties and `minItems` of 0 or 1; a new schema compiles for
  about 90 s; and a schema without `additionalProperties: false` gets a `ValidationException`.
- **O8, live evals.** Run the Evals workflow once with real Bedrock and LangSmith (step 2), and
  check the thresholds, the experiment link and `java-spring-npe`.
- **O9, checks the reviewers could not run.** Watch the `removal` job's first run on GitHub. Try
  `kubectl debug --profile=general` for `python -m asyncio ps` and the Pod Security notes in
  `docs/guides/run-on-eks.md`, and the RDS `GRANT lps_migrator TO <admin>` step. Spot-check the
  numbers in the practice notes that the reviewers did not sample.
