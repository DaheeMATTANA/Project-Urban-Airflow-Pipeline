airflow-up: ## Start Airflow services (webserver + scheduler + postgres)
	docker compose -f platform/airflow/docker/docker-compose.yml up -d

airflow-down: ## Stop Airflow services
	docker compose -f platform/airflow/docker/docker-compose.yml down

redpanda-up: ## Start Redpanda broker
	docker compose -f platform/redpanda/docker-compose.yml up -d

redpanda-down:## Stop Redpanda broker
	docker compose -f platform/redpanda/docker-compose.yml down

minio-up: ## Start MinIO server
	docker compose -f platform/minio/docker-compose.yml up -d

minio-down: ## Start MinIO server
	docker compose -f platform/minio/docker-compose.yml down

all-up: ## Start all services
	$(MAKE) airflow-up
	$(MAKE) redpanda-up
	$(MAKE) minio-up

all-down: ## Stop all services
	$(MAKE) airflow-down
	$(MAKE) redpanda-down
	$(MAKE) minio-down

help: ## Show available make commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ---
# MAKE LINT
# ---
.DEFAULT_GOAL := help

PYTHON := python3

.PHONY: help
help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.PHONY: lint
lint: ## Run Ruff check (same as CI)
	ruff check .

.PHONY: lint-fix
lint-fix: ## Run Ruff with auto-fix locally
	ruff check . --fix

.PHONY: format
format: ## Run Ruff formatter
	ruff format .

.PHONY: precommit
precommit: ## Install pre-commit hooks
	pip install pre-commit && pre-commit install
