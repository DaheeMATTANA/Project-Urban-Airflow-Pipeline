# Airflow
airflow-up:
	docker-compose up -d airflow

airflow-down:
	docker-compose down airflow

# Redpanda (Kafka replacement)
redpanda-up:
	docker-compose up -d redpanda

redpanda-down:
	docker-compose down redpanda

# MinIO
minio-up:
	docker-compose up -d minio

minio-down:
	docker-compose down minio

# Optional: bring everything up/down
all-up:
	docker-compose up -d

all-down:
	docker-compose down