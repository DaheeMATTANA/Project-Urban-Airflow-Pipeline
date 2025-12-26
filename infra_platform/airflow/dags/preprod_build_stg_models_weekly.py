from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dags.common.defaults import DEFAULT_ARGS
from pipelines.loading.dbt_artifacts_duckdb_loader import load_dbt_test_results

"""
## (PREPROD) Build staging models DAG

This DAG runs every week to refresh staging models in preprod.
    - every Monday at 2AM UTC.
    - Logs dbt test results into a materialised table.

Owner: Team Buldo
"""
ENV = "preprod"
DBT_PROJECT_DIR = "/opt/airflow/src/analytics/dbt/urban_airflow_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="preprod_build_stg_models_weekly",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 11 * * MON",  # At 11:30 on Monday
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    tags=["dbt", "staging", "preprod"],
    doc_md=__doc__,
    catchup=False,
) as dag:
    dbt_build_stg = BashOperator(
        task_id="dbt_build_stg",
        bash_command=(
            f"dbt build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--target preprod "
            f"--select models/staging"
        ),
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
            "DBT_DEV_WAREHOUSE_PATH": "/opt/airflow/data/warehouse_dev.duckdb",
        },
    )

    load_dbt_results = PythonOperator(
        task_id="load_dbt_results",
        python_callable=load_dbt_test_results,
        op_kwargs={"env": ENV},
    )

    dbt_build_stg >> load_dbt_results
