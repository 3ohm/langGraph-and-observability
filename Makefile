.DEFAULT_GOAL := help
.PHONY: help setup lint fmt typecheck test

help: ## Show the targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Install the project and every dependency group into .venv
	uv sync --all-groups

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
