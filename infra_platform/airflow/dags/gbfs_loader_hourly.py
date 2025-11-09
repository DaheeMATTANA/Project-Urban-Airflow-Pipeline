from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.common.defaults import DEFAULT_ARGS
from pipelines.loading.gbfs_duckdb_loader import load_gbfs_to_duckdb

"""
## GBFS Loader DAG

Loads GBFS station status from MinIO (bronze) into DuckDB (raw schema).
Runs hourly and applies incremental strategy.

* Important parameter : full_refresh = False will run incremental loads
* and full_refresh needs to be implemented only when it is absolutely necessary.

Owner : Team Buldo
"""


def load_gbfs_task(**context):
    conf = context["dag_run"].conf or {}
    full_refresh = conf.get("full_refresh", False)
    date_str = context["ds"]
    hour = context["logical_date"].hour
    print(f"[INFO] DAG run full_refresh={full_refresh}, date_str={date_str}")
    return load_gbfs_to_duckdb(
        full_refresh=full_refresh, date_str=date_str, hour=hour
    )


with DAG(
    dag_id="gbfs_loader_hourly",
    default_args=DEFAULT_ARGS,
    description="Load GBFS data hourly into DuckDB (raw schema)",
    schedule_interval="@hourly",  # At minute 0
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    tags=["source:gbfs"],
    doc_md=__doc__,
) as dag:
    load_to_duckdb_task = PythonOperator(
        task_id="load_gbfs_to_duckdb",
        python_callable=load_gbfs_task,
    )
