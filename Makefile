# Airflow
airflow-up:
	docker compose -f platform/airflow/docker/docker-compose.yml up -d

airflow-down:
	docker compose -f platform/airflow/docker/docker-compose.yml down

# Redpanda (Kafka replacement)
redpanda-up:
	docker compose -f platform/redpanda/docker-compose.yml up -d

redpanda-down:
	docker compose -f platform/redpanda/docker-compose.yml down

# MinIO
minio-up:
	docker compose -f platform/minio/docker-compose.yml up -d

minio-down:
	docker compose -f platform/minio/docker-compose.yml down

# Optional: bring everything up/down
all-up: airflow-up redpanda-up minio-up

all-down: airflow-down redpanda-down minio-down