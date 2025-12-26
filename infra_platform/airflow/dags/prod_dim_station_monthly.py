from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dags.common.defaults import DEFAULT_ARGS
from pipelines.loading.dbt_artifacts_duckdb_loader import load_dbt_test_results

"""
## (PROD) Build dim_station model DAG

This DAG runs every week to refresh fct_station_status in prod.
    - runs all the upstream models
    - every 1st day of the month at 2:00AM UTC.
    - Logs dbt test results into a materialised table.

Owner: Team Buldo
"""

ENV = "prod"
DBT_PROJECT_DIR = "/opt/airflow/src/analytics/dbt/urban_airflow_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="prod_dim_station_monthly",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 1 * *",  # At 02:00 on day-of-month 1
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    tags=["dbt", "dim", "station", "prod"],
    doc_md=__doc__,
    catchup=False,
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"dbt build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--target prod "
            f"--select +dim_station "
            f"--exclude models/staging"
        ),
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
            "DBT_DEV_WAREHOUSE_PATH": "/opt/airflow/data/warehouse_dev.duckdb",
        },
    )

    load_dbt_results = PythonOperator(
        task_id="load_dbt_results",
        python_callable=load_dbt_test_results,
        op_kwargs={
            "env": ENV,
        },
    )

    dbt_build >> load_dbt_results
