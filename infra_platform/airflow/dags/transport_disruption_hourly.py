from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.common.defaults import DEFAULT_ARGS
from pipelines.ingestion.transport_disruption_ingest import (
    fetch_and_store_idfm_data,
)
from pipelines.loading.transport_disruption_duckdb_loader import (
    DisruptionLoader,
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


def load_disruptions_partition(**context):
    conf = context["dag_run"].conf or {}
    full_refresh = conf.get("full_refresh", False)
    date_str = context["ds"]
    hour = context["logical_date"].hour

    loader = DisruptionLoader()
    count = loader.load_partition(
        date_str=date_str, hour=hour, full_refresh=full_refresh
    )

    context["task_instance"].log.info(
        f"[INFO] Inserted {count} rows into {loader.table_name} for {date_str} hour {hour} (full_refresh={full_refresh})"
    )
    return count


with DAG(
    dag_id="transport_disruption_hourly",
    default_args=DEFAULT_ARGS,
    description="Ingest and load IDFM disruption data",
    schedule_interval="@hourly",
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    tags=["source:IDFM", "bronze"],
) as dag:
    ingest_task = PythonOperator(
        task_id="fetch_and_store_idfm_data",
        python_callable=fetch_and_store_idfm_data,
        provide_context=True,
    )

    loading_task = PythonOperator(
        task_id="load_partition_to_duckdb",
        python_callable=load_disruptions_partition,
        provide_context=True,
        op_kwargs={
            "date_filter": "{{ ds }}"  # Use Airflow execution date
        },
    )

ingest_task >> loading_task
