from airflow import DAG
from airflow.operators.bash import BashOperator
from dags.common.defaults import DEFAULT_ARGS

"""
## Build dbt models ad hoc DAG

This DAG runs dbt models in a flexible way:
    - You can pass `dbt_select` to specify which models to build.
    - Optional `dbt_exclude` to exclude certain models.

Owner: Team Buldo
"""

DBT_PROJECT_DIR = "/opt/airflow/src/analytics/dbt/urban_airflow_analytics"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_ad_hoc_manually",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # Trigger manually or via params
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    tags=["dbt", "ad_hoc", "dev"],
    doc_md=__doc__,
    catchup=False,
) as dag:
    dbt_select = "{{ dag_run.conf.get('dbt_select', 'dim_calendar') }}"
    dbt_exclude = "{{ dag_run.conf.get('dbt_exclude', 'models/staging') }}"
    dbt_target = "{{ dag_run.conf.get('dbt_target', 'dev') }}"
    full_refresh = "{{ dag_run.conf.get('full_refresh', 'false') }}"

    full_refresh_flag = "{% if dag_run.conf.get('full_refresh', 'false').lower() == 'true' %}--full-refresh{% endif %}"

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"dbt build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--target {dbt_target} "
            f"--select {dbt_select} "
            f"--exclude {dbt_exclude} "
            f"{full_refresh_flag}"
        ),
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
            "DBT_DEV_WAREHOUSE_PATH": "/opt/airflow/data/warehouse_dev.duckdb",
        },
    )
