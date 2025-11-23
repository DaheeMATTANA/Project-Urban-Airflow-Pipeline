# Airflow DAGs Overview

This folder contains the DAGs orchestrated by Apache Airflow.
Each DAG automates part of the data platform workflow — from ingestion to transformation and warehouse management.

---

## 1. DBT Build DAGs

### Description
These DAGs manage the automated execution of dbt models across different environments (dev, preprod, prod) on recurring schedules.

### DAG List

| DAG ID | Environment | Schedule | Description |
|--------|--------------|-----------|--------------|
| `dbt_ad_hoc_manually` | Manual | None | Runs dbt models on demand. You can specify `dbt_select` and optionally `dbt_exclude` for flexible builds. |
| `dev_build_stg_models_weekly` | Dev | Every Monday at 10:30 UTC | Refreshes staging models in the dev environment and logs dbt test results. |
| `preprod_build_stg_models_weekly` | Preprod | Every Monday at 11:30 UTC | Refreshes staging models in preprod and logs dbt test results. |
| `prod_build_stg_models_daily` | Prod | Everyday at 12:30 UTC | Refreshes staging models in prod and logs dbt test results. |
| `prod_dim_calendar_yearly` | Prod | Every January 1st at 00:00 UTC | Rebuilds the `dim_calendar` model and its upstream dependencies yearly. |
| `prod_station_status_daily` | Prod | Everyday at 13:15 UTC | Refreshes `fct_hourly_station_status` and its upstream dependencies daily. |
| `prod_dim_station_monthly` | Prod | Every 1st day of month at 02:00 UTC | Refreshes `dim_station` and its upstream dependencies monthly. |
| `prod_transport_disruption_daily` | Prod | Everyday at 13:30 UTC | Refreshes `fct_daily_transport_disruption` and its upstream dependencies daily. |
| `prod_weather_daily` | Prod | Everyday at 13:45 UTC | Refreshes `fct_hourly_weather` and its upstream dependencies daily. |

---

## 2. GBFS (Bike Sharing) DAGs

### Description
These DAGs orchestrate the ingestion and processing of GBFS (General Bikeshare Feed Specification) data, covering real-time streams, backfills, and metadata ingestion.

### DAG List

| DAG ID | Frequency | Description |
|--------|------------|-------------|
| `gbfs_stream_every_min` | Every minute | Produces GBFS station status messages into Kafka (Redpanda) and logs metadata to DuckDB. |
| `gbfs_consumer_5_min` | Every 5 minutes | Consumes GBFS live station data from API and stores raw data in MinIO (bronze). |
| `gbfs_loader_hourly` | Every hour | Loads GBFS station status from MinIO into DuckDB (raw schema). Supports incremental loads. |
| `gbfs_station_information_monthly` | Every month | Ingests GBFS station information and loads it into DuckDB (`raw_gbfs_station_information`). |
| `gbfs_backfill_producer_manually` | Manual | Replays historical GBFS snapshots into Kafka using the `?at=<timestamp>` parameter. |

---

## 3. Open Weather and Air Quality DAGs

### Description
These DAGs handle ingestion of public weather and air quality data sources (Open Meteo and OpenAQ) into MinIO and DuckDB for analytics use.

### DAG List

| DAG ID | Frequency | Description |
|--------|------------|-------------|
| `open_meteo_hourly` | Every hour | Ingests Open Meteo data into MinIO and DuckDB. Supports incremental or full loads. |
| `openaq_hourly` | Every hour | Ingests OpenAQ air quality data into MinIO and DuckDB. Supports incremental or full loads. |

---

## 4. IDFM Transport Disruption DAGs

### Description
These DAGs manage ingestion and backfill of **Île-de-France Mobilités (IDFM)** transport disruption data.

### DAG List

| DAG ID | Frequency | Description |
|--------|------------|-------------|
| `transport_disruption_hourly` | Every hour | Ingests and loads IDFM transport disruptions into DuckDB (raw). Supports incremental loads. |
| `transport_disruption_backfill_manually` | Manual | Backfills existing MinIO files into DuckDB for IDFM data (no API call). |

---

## 5. France Holidays DAG

### Description
Handles ingestion of official French public holidays into the data warehouse for time-based analytics.

### DAG List

| DAG ID | Frequency | Description |
|--------|------------|-------------|
| `france_holidays_quarterly` | Every quarter | Ingests official French holidays data into DuckDB. |

---

## 6. Warehouse and Snapshot Management

### Description
Automates snapshots of DuckDB warehouses and uploads them to MinIO for CI/CD pipelines and cross-environment testing.

### DAG List

| DAG ID | Frequency | Description |
|--------|------------|-------------|
| `snapshot_warehouse_dev_daily` | Daily at 02:00 UTC | Uploads `warehouse_dev.duckdb` snapshot to MinIO for CI/CD pipelines. |


---

## Summary

| Category | Number of DAGs |
|-----------|----------------|
| DBT Models | 9 |
| GBFS Ingestion | 5 |
| Weather & Air Quality | 2 |
| IDFM Transport | 2 |
| Holidays | 1 |
| Snapshots | 1 |
| **Total** | **20 DAGs** |
