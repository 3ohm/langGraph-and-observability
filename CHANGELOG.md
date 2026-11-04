# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Versions stay at 0.x until
launch.

## [Unreleased]

## [0.1.0] - pending owner smoke test

The first release (Phases 0 and 1). It is tagged after the owner's real-mode smoke test
(`docs/release-checklist.md`), whose date and result go here.

### Added

- Project skeleton: a uv project on Python 3.14 with ruff, mypy (strict), pytest (warnings are
  errors, coverage reported), pre-commit hooks, a Makefile and the governance files.
- Postgres persistence: a health-checked connection pool with a PgBouncer-safe mode, a pool-aware
  checkpointer, strict serialization with an allowlist and optional encryption at rest,
  forward-only plain-SQL migrations as their own step, an action ledger for idempotent side
  effects, two database roles (`deploy/sql/roles.sql`) and leader-elected retention for
  checkpoints and results.
- The queue worker: SQS intake that validates, dedups and records a message before it deletes
  it, with bad messages left to the dead-letter queue; runs with leases, heartbeats and periodic
  recovery; a run deadline; a drain within Kubernetes' 60-second shutdown hook; resume at the
  failed step; health endpoints (`/ok`, `/ready`); and an operator CLI (`migrate`, `runs list`,
  `runs show`, `runs retry`).
- Plain LangGraph: `durability="sync"`, explicit retry, timeout and error-handler policies, a
  `context_schema`, an agent registry, and a test-only graph that proves the core practices.
- LangSmith: one redacting client, trace content modes (`full`, `redacted`, `none`) and run
  metadata.
- The Incident Triage example agent: Kubernetes and Datadog evidence, deterministic assessment
  rules, code evidence at the deployed commit (workload annotations, ECR image labels or a source
  map; Git checkouts from allowlisted hosts only, with a host-scoped token), stack-trace parsers
  for the JVM, Node.js, Python, Go, PHP, Ruby and native code, tree-sitter extraction, ripgrep
  search, a bounded code bundle, redaction before the model, and a typed analysis by Claude on
  Amazon Bedrock with validated citations, one repair retry and refusal handling. Results go to
  Postgres and to the `log` and `sqs` sinks, each once. Real and fake adapters for every external
  system.
- Sample apps and evals: twelve sample apps in eleven languages with real crash traces, sixteen
  incident fixtures with recorded replies, evals that gate pull requests, and a live eval
  workflow against Bedrock, gated on a repository variable.
- Deployment: a Dockerfile (`python:3.14-slim` with ripgrep, git and the grammars), a Compose
  stack (Postgres, ElasticMQ, migrate, worker), example EKS manifests (kustomize) with read-only
  RBAC, and an IAM policy.
- Docs and scripts: 23 practice notes; the quickstart, run-on-eks, real-mode, customize and
  upgrade guides; a release checklist; `scripts/rename.py`, `scripts/remove_example.py` and
  `scripts/send_incident.py`; CI with lint, unit, integration, end-to-end, image, removal and
  Python 3.15 canary jobs.
