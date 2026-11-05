.DEFAULT_GOAL := help
.PHONY: help setup up down migrate studio lint pylint fmt typecheck test test-int test-e2e

help: ## Show the targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z0-9-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Install the project and every dependency group into .venv, then fetch the grammars
	uv sync --all-groups
	$(MAKE) grammars

# example-agent:begin (scripts/remove_example.py deletes this block)
.PHONY: grammars
grammars: ## Fetch the tree-sitter grammars into APP_GRAMMAR_DIR (default: the user cache)
	uv run python -m langgraph_and_observability.agents.incident_triage.codeintel.languages --download
# example-agent:end

up: ## Build the image; start postgres, elasticmq, migrate and worker (compose.yaml)
	docker compose up -d --build

down: ## Stop the local stack; its database volume stays (docker compose down -v deletes it)
	docker compose down

migrate: ## Rebuild the image and apply the migrations to the local stack's Postgres
	docker compose run --rm --build migrate

# example-agent:begin (scripts/remove_example.py deletes this block)
# make incident LANG=<language> sends the incident of that language's sample app, and
# make incident FIXTURE=<id> sends any incident fixture (spec 10.2). FIXTURE wins.
# Each entry of INCIDENTS is <language>:<fixture ID>.
INCIDENTS := javascript:js-typeerror typescript:ts-undefined-property python:python-keyerror \
	java:java-spring-npe go:go-nil-map php:php-division-by-zero ruby:ruby-nomethod \
	kotlin:kotlin-illegal-state scala:scala-match-error c:c-segfault cpp:cpp-out-of-range
INCIDENT_LANGS = $(foreach entry,$(INCIDENTS),$(firstword $(subst :, ,$(entry))))
# LANG is also the locale variable, and make passes a LANG from its command line to every
# recipe. So only a command-line LANG names a language, and the recipe runs the script
# with LANG=C.UTF-8: no program gets LANG=python as its locale.
INCIDENT_LANG = $(if $(filter command line,$(origin LANG)),$(LANG))
INCIDENT_FIXTURE = $(or $(FIXTURE),$(patsubst $(INCIDENT_LANG):%,%,$(filter $(INCIDENT_LANG):%,$(INCIDENTS))))
INCIDENT_USAGE = make incident needs FIXTURE=<id> or LANG=<language>. The languages: $(INCIDENT_LANGS)

.PHONY: incident
incident: ## Send an incident to the local stack: LANG=<language> (python, go, ...) or FIXTURE=<id>
	LANG=C.UTF-8 uv run scripts/send_incident.py --fixture $(or $(INCIDENT_FIXTURE),$(error $(INCIDENT_USAGE)))
# example-agent:end

studio: ## Serve the graphs of langgraph.json to LangGraph Studio (langgraph dev, port 2024)
	uv run langgraph dev --no-browser

lint: ## Lint and check formatting (ruff), then lint with pylint (make pylint)
	uv run ruff check .
	uv run ruff format --check .
	$(MAKE) --no-print-directory pylint

# make pylint runs pylint twice with the rules in pyproject.toml: on package code and
# scripts, then on the test modules with the checks in PYLINT_TEST_DISABLE also off.
# The test modules: tests/, the root conftest.py, and each agent's tests/ and evals/test_*.py.
PYLINT_TESTS = tests conftest.py $(wildcard src/*/agents/*/tests src/*/agents/*/evals/test_*.py)
# pytest idioms trip these checks in test modules:
# a test's name says what it checks;
PYLINT_TEST_DISABLE := missing-module-docstring missing-class-docstring missing-function-docstring
# pytest passes fixtures as arguments by name, some only for their side effects, and fakes
# take the arguments of what they replace;
PYLINT_TEST_DISABLE += redefined-outer-name unused-argument too-many-positional-arguments
# `assert items == []` shows the items when it fails, and `assert type(x) is str` checks the
# exact type; `[item] = items` checks that there is one item (pylint can infer an empty list);
PYLINT_TEST_DISABLE += use-implicit-booleaness-not-comparison unidiomatic-typecheck
PYLINT_TEST_DISABLE += unbalanced-tuple-unpacking
# tests check and patch private helpers;
PYLINT_TEST_DISABLE += protected-access
# fixtures import Docker and AWS libraries only when a test asks for them;
PYLINT_TEST_DISABLE += import-outside-toplevel
# and tests repeat expected values and setups on purpose.
PYLINT_TEST_DISABLE += duplicate-code

pylint: ## Lint package code, then the tests with fewer checks (pylint, pyproject.toml)
	uv run pylint src scripts --ignore=tests --ignore-patterns='^test_'
	uv run pylint $(PYLINT_TESTS) $(addprefix --disable=,$(PYLINT_TEST_DISABLE))

fmt: ## Apply ruff fixes, then format
	uv run ruff check --fix --exit-zero .
	uv run ruff format .

typecheck: ## Type-check src, tests, scripts and the root conftest.py (mypy, strict)
	uv run mypy src tests scripts conftest.py

test: ## Unit tests with coverage (no Docker, no network)
	uv run pytest -m "not integration and not e2e" --cov=langgraph_and_observability --cov-report=term-missing

test-int: ## Integration tests against Postgres and ElasticMQ (needs Docker)
	uv run pytest -m integration

test-e2e: ## End-to-end tests: worker subprocesses, kill -9, SIGTERM, simulated triage (needs Docker)
	uv run pytest -m e2e
