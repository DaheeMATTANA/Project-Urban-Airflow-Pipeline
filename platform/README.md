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

## 🚀 Service Management (Makefile)

This project provides a `Makefile` to easily manage common services such as **Airflow**, **Redpanda**, and **MinIO**.  
It simplifies workflows with one-liners to start, stop, and monitor everything.

---

### 📦 Prerequisites

- [GNU Make](https://www.gnu.org/software/make/)  
- [Docker & Docker Compose](https://docs.docker.com/)  
- On Windows: Git Bash or WSL2 is recommended (see ⚠️ note below)

---

### ▶️ Usage

All available commands:

```bash
make airflow-up     # Start Airflow (webserver, scheduler, Postgres)
make airflow-down   # Stop Airflow

make redpanda-up    # Start Redpanda broker
make redpanda-down  # Stop Redpanda

make minio-up       # Start MinIO
make minio-down     # Stop MinIO

make all-up         # Start everything
make all-down       # Stop everything

make help           # Show available make commands
