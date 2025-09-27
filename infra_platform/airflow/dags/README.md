# Airflow DAGs

This folder contains the DAGs orchestrated by Apache Airflow.

## DAG List

| DAG ID                               | Description                 | Schedule    |
|--------------------------------------|-----------------------------|-------------|
| france_holidays_quarterly            | Holidays ingestion          | Quarterly   |
| gbfs_backfill_producer_manually      | Backfill GBFS               | Manual      |
| gbfs_consumer_5_min                  | Source: GBFS                | Every 5 min |
| gbfs_loader_hourly                   | Source: GBFS                | Hourly      |
| gbfs_stream_every_min                 | Produce: GBFS               | Every min   |
| healthcheck                          | Airflow healthcheck         | Manual      |
| openaq_hourly                        | Source: OpenAQ              | Hourly      |
| open_meteo_hourly                    | Source: Open Meteo          | Hourly      |
| stream_dummy_every_min               | Kafka / Redpanda streaming  | Every min   |
| transport_disruption_backfill_manually | Backfill bronze source: IDFM | Manual    |
| transport_disruption_hourly          | Source: IDFM disruptions    | Hourly      |
