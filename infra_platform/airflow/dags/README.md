# Airflow DAGs

This folder contains the DAGs orchestrated by Apache Airflow.

## DAG List

| DAG ID                               | Description                 | Owner      | Schedule        |
|--------------------------------------|-----------------------------|------------|-----------------|
| `france_holidays_quarterly`          | Holidays ingestion          | team-buldo | `0 0 1 */3 *`   |
| `gbfs_backfill_producer_manually`    | Backfill GBFS               | team-buldo | Manual          |
| `gbfs_consumer_5_min`                | Source: GBFS                | team-buldo | `*/5 * * * *`   |
| `gbfs_loader_hourly`                 | Source: GBFS                | team-buldo | `@hourly`       |
| `gbfs_stream_every_min`              | Produce: GBFS               | team-buldo | `* * * * *`     |
| `healthcheck`                        | Airflow healthcheck         | airflow    | Manual          |
| `openaq_hourly`                      | Source: OpenAQ              | team-buldo | `@hourly`       |
| `open_meteo_hourly`                  | Source: Open Meteo          | team-buldo | `@hourly`       |
| `stream_dummy_every_min`             | Kafka / Redpanda streaming  | airflow    | `* * * * *`     |
| `transport_disruption_backfill_manually` | Backfill bronze source: IDFM | airflow | Manual          |
| `transport_disruption_hourly`        | Source: IDFM disruptions    | airflow    | `@hourly`       |
