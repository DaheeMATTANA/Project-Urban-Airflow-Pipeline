from airflow import DAG
from airflow.operators.bash import BashOperator
from dags.common.defaults import DEFAULT_ARGS

"""
## Build staging models DAG

This DAG runs every week to refresh staging models in dev.
    - every Monday at 1AM UTC.

Owner: Team Buldo
"""

DBT_PROJECT_DIR = "/opt/airflow/src/analytics/dbt/urban_airflow_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="build_stg_models_weekly",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * MON",  # every Monday at 1AM
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    tags=["dbt", "staging"],
    doc_md=__doc__,
) as dag:
    dbt_build_stg = BashOperator(
        task_id="dbt_build_stg",
        bash_command=(
            f"dbt build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--target dev "
            f"--select models/staging"
        ),
    )
