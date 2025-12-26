from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.common.defaults import DEFAULT_ARGS
from pipelines.ingestion.gbfs_station_info_ingest import run_ingest
from pipelines.loading.gbfs_station_info_duckdb_loader import (
    run_station_info_load,
)

"""
## GBFS Station Information DAG

This DAG runs every month to ingest station information from GBFS.
This DAG
    - writes to MinIO
    - load data to duckDB into raw.raw_gbfs_station_information

Owner: Team Buldo
"""

with DAG(
    dag_id="gbfs_station_information_monthly",
    default_args=DEFAULT_ARGS,
    description="Ingest and load gbfs station information",
    schedule_interval="0 0 1 * *",  # At 00:00 on day-of-month 1
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    tags=["source:gbfs"],
    doc_md=__doc__,
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="fetch_and_store",
        python_callable=run_ingest,
    )

    load_task = PythonOperator(
        task_id="load_into_duckdb",
        python_callable=run_station_info_load,
    )

    ingest_task >> load_task
