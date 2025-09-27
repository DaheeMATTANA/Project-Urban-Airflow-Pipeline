airflow-up: ## Start Airflow services (webserver + scheduler + postgres)
	docker compose -f infra_platform/airflow/docker/docker-compose.yml up -d

airflow-down: ## Stop Airflow services
	docker compose -f infra_platform/airflow/docker/docker-compose.yml down

redpanda-up: ## Start Redpanda broker
	docker compose -f infra_platform/redpanda/docker-compose.yml up -d

redpanda-down:## Stop Redpanda broker
	docker compose -f infra_platform/redpanda/docker-compose.yml down

minio-up: ## Start MinIO server
	docker compose -f infra_platform/minio/docker-compose.yml up -d

minio-down: ## Start MinIO server
	docker compose -f infra_platform/minio/docker-compose.yml down

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


# ---
# Run Spark
# ---
run-spark: ## Run Spark with dependencies (argument FILE=path/to/your/script.py)
	spark-submit \
	  --master local[*] \
	  --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.12.262 \
	  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	  --conf spark.hadoop.fs.s3a.path.style.access=true \
	  $(FILE)

run-openaq: ## Run openaq_spark_ingest.py
	make run-spark FILE=infra_platform/pipelines/ingestion/openaq_spark_ingest.py

# ---
# Download duckdb artifacts from git
# ---
REPO = DaheeMATTANA/Project-Urban-Airflow-Pipeline

download-preprod: ## Download warehouse_preprod.duckdb
	rm -f infra_platform/duckdb_data/warehouse_preprod.duckdb
	gh run download --repo $(REPO) -n warehouse_preprod -D infra_platform/duckdb_data

download-prod: ## Download warehouse_prod.duckdb
	rm -f infra_platform/duckdb_data/warehouse_prod.duckdb
	gh run download --repo $(REPO) -n warehouse_prod -D infra_platform/duckdb_data