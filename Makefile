.DEFAULT_GOAL := help
.PHONY: help setup grammars up down migrate incident studio lint fmt typecheck test test-int test-e2e

help: ## Show the targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z0-9-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Install the project and every dependency group into .venv, then fetch the grammars
	uv sync --all-groups
	$(MAKE) grammars

grammars: ## Fetch the tree-sitter grammars into APP_GRAMMAR_DIR (default: the user cache)
	uv run python -m langgraph_postgres_starter.agents.incident_triage.codeintel.languages --download

up: ## Build the image; start postgres, elasticmq, migrate and worker (compose.yaml)
	docker compose up -d --build

down: ## Stop the local stack; its database volume stays (docker compose down -v deletes it)
	docker compose down

migrate: ## Rebuild the image and apply the migrations to the local stack's Postgres
	docker compose run --rm --build migrate

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

incident: ## Send an incident to the local stack: LANG=<language> (python, go, ...) or FIXTURE=<id>
	LANG=C.UTF-8 uv run scripts/send_incident.py --fixture $(or $(INCIDENT_FIXTURE),$(error $(INCIDENT_USAGE)))

studio: ## Serve the incident_triage graph to LangGraph Studio (langgraph dev, port 2024)
	uv run langgraph dev --no-browser

lint: ## Lint and check formatting (ruff)
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Apply ruff fixes, then format
	uv run ruff check --fix --exit-zero .
	uv run ruff format .

typecheck: ## Type-check src, tests, scripts and the root conftest.py (mypy, strict)
	uv run mypy src tests scripts conftest.py

test: ## Unit tests with coverage (no Docker, no network)
	uv run pytest -m "not integration and not e2e" --cov=langgraph_postgres_starter --cov-report=term-missing

test-int: ## Integration tests against Postgres and ElasticMQ (needs Docker)
	uv run pytest -m integration

test-e2e: ## End-to-end tests: worker subprocesses, kill -9, SIGTERM, simulated triage (needs Docker)
	uv run pytest -m e2e
