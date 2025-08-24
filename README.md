# Project-Urban-Airflow-Pipeline
End-to-end data pipeline project on urban mobility and weather condition.

# Urban Airflow Pipeline

**Urban Airflow Pipeline** is a modern, end-to-end **data engineering & analytics engineering** project that ingests, processes, and visualises real-time urban mobility, weather, and air quality data.

This repository is designed as a **portfolio-quality example** of a production-style data platform — but fully runnable locally and cost-free — while mirroring cloud-native best practices.

---

## 🎯 Scope

Urban Airflow Pipeline covers the full lifecycle of a modern data product:

1. **Streaming ingestion**  
   - Real-time bike-sharing station status via Kafka/Redpanda.
   - Batch ingestion of weather & air quality APIs.

2. **Data orchestration**  
   - Apache Airflow for scheduling, dependency management, and data quality checks.

3. **Data storage & lakehouse layers** *(local/cloud-ready)*  
   - Bronze/Silver/Gold layers via MinIO (local S3) or GCS (cloud).
   - Warehouse layer in DuckDB (local) or BigQuery/Snowflake (cloud).

4. **Data transformation & testing**  
   - dbt for ELT modelling (staging, marts, dimensional models).
   - Great Expectations for automated data quality checks.

5. **Analytics & BI**  
   - Power BI dashboard showing live metrics: station availability, weather, air quality, and correlations.

6. **DevOps & CI/CD**  
   - Terraform for infrastructure-as-code (cloud deployment).
   - GitHub Actions for linting, testing, and pipeline deployment.
   - Local Docker-based development for cost-free iteration.

---

## 📦 Tech Stack

- **Orchestration:** Apache Airflow (local via Docker; cloud via Composer)
- **Streaming:** Redpanda (Kafka API-compatible)
- **Batch ingestion:** Python (Requests, Pandas)
- **Storage:** MinIO (local S3), GCS (cloud)
- **Warehouse:** DuckDB (local), BigQuery/Snowflake (cloud)
- **Transformation:** dbt Core + dbt-duckdb / dbt-bigquery
- **Data quality:** Great Expectations
- **BI:** Power BI
- **IaC:** Terraform
- **CI/CD:** GitHub Actions
- **Observability:** Airflow UI, logs, lineage (OpenLineage-ready)

---

## 🗂 Repo Structure

<pre>
infra_platform/          # Infra, orchestration, pipelines
  airflow/         # Airflow DAGs, plugins, docker setup
  pipelines/       # Ingestion scripts (batch & stream)
  minio/           # Local object storage config
  redpanda/        # Local streaming config

analytics/         # dbt project & BI assets
  dbt/             # dbt project
  bi/powerbi/      # PBIX files and dataset docs
</pre>

---

## 🚀 Goals

- Showcase **both** data engineering and analytics engineering skills.
- Mirror a **production-grade** architecture with cloud-native patterns.
- Keep everything runnable locally to avoid cloud costs during development.
- Provide a clear path to deploy the same stack on **GCP + BigQuery** with minimal changes.

---

## 🔗 Related

- **Urban Airflow Analytics** (optional separate repo): dbt + BI only, for teams splitting DE/AE workflows.
- Public API sources: GBFS (bike sharing), OpenWeather, OpenAQ.

---

## 📝 Status

**Phase:** S2 — Ingestion.

