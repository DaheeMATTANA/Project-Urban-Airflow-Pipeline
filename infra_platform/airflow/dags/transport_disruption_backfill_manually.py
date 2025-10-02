from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.loading.transport_disruption_duckdb_loader import (
    DisruptionLoader,
)

"""
## IDFM transport disruption backfill DAG

This DAG backfills duckdb loader job for IDFM data :
- Assuming that the files exist in MinIO
- Only loads the data into DuckDB
- Does not call API or write to MinIO

Owner : Team Buldo
"""


def backfill_disruptions(**context):
    conf = context["dag_run"].conf or {}
    start_date = datetime.strptime(conf.get("start_date"), "%Y-%m-%d")
    end_date = datetime.strptime(conf.get("end_date"), "%Y-%m-%d")

    loader = DisruptionLoader()
    date = start_date
    while date <= end_date:
        for hour in range(0, 24):
            print(
                f"[INFO] Backfilling {date.strftime('%Y-%m-%d')} hour {hour}"
            )
            loader.load_partition(
                date_str=date.strftime("%Y-%m-%d"),
                hour=hour,
                full_refresh=True,
            )
        date += timedelta(days=1)


with DAG(
    dag_id="transport_disruption_backfill_manually",
    start_date=datetime(2025, 9, 24),
    description="Backfill IDFM disruption data from MinIO into DuckDB",
    schedule_interval=None,  # only runs when manually triggered
    catchup=False,
    tags=["source:IDFM", "bronze", "backfill"],
    doc_md=__doc__,
) as dag:
    backfill_task = PythonOperator(
        task_id="backfill_disruptions",
        python_callable=backfill_disruptions,
        provide_context=True,
    )
