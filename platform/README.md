# Platform

This folder contains the **data engineering platform layer** of Urban Airflow Pipeline.

It includes all components related to infrastructure, orchestration, and ingestion.

---

## 📂 Structure
- **airflow/** → DAGs, plugins, and Docker config to run Airflow locally.
- **pipelines/** → Python code for ingestion & streaming (batch APIs, producers/consumers).
- **minio/** → Local object storage (S3-compatible) replacing GCS buckets.
- **redpanda/** → Local Kafka-compatible broker for streaming.

---

## 🚀 Goals
- Provide a **local-first stack** that mirrors cloud-native architecture.
- Enable **streaming ingestion** (Redpanda) and **batch pipelines** (Airflow).
- Store data in **bronze/silver/gold/logs** buckets via MinIO.

---

# 🚀 Service Management (Makefile)

To simplify workflows, common services can be started/stopped with one-liners.

Start / Stop
make airflow-up     # start Airflow (webserver, scheduler, Postgres)
make airflow-down   # stop Airflow

make redpanda-up    # start Redpanda broker
make redpanda-down  # stop Redpanda

make minio-up       # start MinIO
make minio-down     # stop MinIO

make all-up         # start everything
make all-down       # stop everything

Help
make help


Outputs a list of available commands with descriptions:

airflow-up      Start Airflow services (webserver + scheduler + postgres)
airflow-down    Stop Airflow services
redpanda-up     Start Redpanda broker
redpanda-down   Stop Redpanda broker
minio-up        Start MinIO server
minio-down      Stop MinIO server
all-up          Bring everything up
all-down        Bring everything down
help            Show available make commands


⚠️ On Windows (PowerShell/CMD), make help requires Git Bash or WSL2 (because it uses grep + awk).