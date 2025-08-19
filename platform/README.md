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
