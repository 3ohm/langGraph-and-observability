# ADR 0001: A template, not a framework

- **Status:** Accepted
- **Date:** 2026-11-03
- **Source:** design spec, `docs/design/2026-11-01-langgraph-and-observability-design.md`: §3 goal 1,
  §4 (non-goals) and §5 (anti-goals)

## Context

This project shows how to build LangGraph agents that hold up in production: Postgres persistence,
crash recovery, retention, LangSmith tracing and evals. LangGraph's own docs stop at beginner-level
persistence, so people need working code, not only advice.

The code can take one of two shapes:

- **A framework:** a pip-installable library with its own layer over LangGraph. Users depend on it
  and upgrade it.
- **A template:** a repository that people fork, strip down and change. Users own the code.

## Decision

This project is a template. The code is plain LangGraph and Python, with no wrapper layer.

- It is not published as a pip-installable framework (spec §4). The Python package exists so that
  the app can be built, tested and shipped as a container, not as a library API.
- No helper may hide LangGraph's APIs (spec §5). A reader of LangGraph's docs can read this code.
- Every production practice is real code with a test and a short note, so a fork can keep or drop
  each one on its own.
- Parts are removable: the example agent and the worker can be deleted, and a script renames the
  package for a fork (spec §13.9).

## Consequences

- Good: users change the code directly, LangGraph's docs apply as written, and LangGraph upgrades
  stay visible in the diff.
- Good: coding assistants can extend a fork with only `AGENTS.md` and the code in front of them.
- Bad: there is no upgrade through a package version. A fork pulls later changes from this
  repository by hand.
- Bad: a second agent may repeat some code instead of calling a shared helper. We accept that
  repetition over a layer that hides LangGraph.
- A fork that takes a day to strip down counts as a failure (spec §5). CI must prove that the
  example agent can be removed (spec §13.3).
