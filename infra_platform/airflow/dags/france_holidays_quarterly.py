from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.common.defaults import DEFAULT_ARGS
from pipelines.ingestion.france_holidays_ingest import run_ingest
from pipelines.loading.france_holidays_duckdb_loader import HolidaysLoader

"""
## Franc holidays ingestion DAG

This DAG runs every quarter to ingest holiday data in France.

Owner: Team Buldo
"""


def load_into_duckdb(min_year: int):
    loader = HolidaysLoader(min_year=min_year)
    loader.load_json_files()


with DAG(
    dag_id="france_holidays_quarterly",
    default_args=DEFAULT_ARGS,
    description="Ingest and load French public holidays",
    schedule_interval="0 0 1 */3 *",  # every quarter
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    catchup=True,
    tags=["holidays", "ingestion"],
    doc_md=__doc__,
) as dag:
    ingest_task = PythonOperator(
        task_id="fetch_and_store_holidays",
        python_callable=run_ingest,
        op_kwargs={"min_year": 2025},
    )

    load_task = PythonOperator(
        task_id="load_holidays_into_duckdb",
        python_callable=load_into_duckdb,
        op_kwargs={"min_year": 2025},
    )

    ingest_task >> load_task
