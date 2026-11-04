.DEFAULT_GOAL := help
.PHONY: help setup up down migrate lint fmt typecheck test test-int test-e2e

help: ## Show the targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z0-9-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Install the project and every dependency group into .venv
	uv sync --all-groups

up: ## Build the image; start postgres, elasticmq, migrate and worker (compose.yaml)
	docker compose up -d --build

down: ## Stop the local stack; its database volume stays (docker compose down -v deletes it)
	docker compose down

migrate: ## Rebuild the image and apply the migrations to the local stack's Postgres
	docker compose run --rm --build migrate

lint: ## Lint and check formatting (ruff)
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Apply ruff fixes, then format
	uv run ruff check --fix --exit-zero .
	uv run ruff format .

typecheck: ## Type-check src and tests (mypy, strict)
	uv run mypy src tests

test: ## Unit tests with coverage (no Docker, no network)
	uv run pytest -m "not integration and not e2e" --cov=langgraph_postgres_starter --cov-report=term-missing

test-int: ## Integration tests against Postgres and ElasticMQ (needs Docker)
	uv run pytest -m integration

test-e2e: ## End-to-end tests: kill -9 and SIGTERM against worker subprocesses (needs Docker)
	uv run pytest -m e2e
