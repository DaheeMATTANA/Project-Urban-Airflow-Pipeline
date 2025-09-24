from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.common.defaults import DEFAULT_ARGS
from pipelines.ingestion.transport_disruption_ingest import (
    fetch_and_store_idfm_data,
)

"""
## IDFM transport disruption ingestion & loading DAG

This DAG ingests **IDFM disruption data** every hour :
- Fetches transport disruption data from provider API
- Stores raw data in MinIO (bronze)
- Pushes metadata to DuckDB raw

* Important parameter : full_refresh = False will run incremental loads
* and full_refresh needs to be implemented only when it is absolutely necessary.

Owner : Team Buldo
"""

with DAG(
    dag_id="transport_disruption_hourly",
    default_args=DEFAULT_ARGS,
    description="Ingest and load IDFM disruption data",
    schedule_interval="@hourly",
    catchup=False,
    tags=["source:IDFM", "bronze"],
) as dag:
    ingest_task = PythonOperator(
        task_id="fetch_and_store_idfm_data",
        python_callable=fetch_and_store_idfm_data,
    )
