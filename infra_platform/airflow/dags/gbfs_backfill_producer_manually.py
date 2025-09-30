from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.streaming.gbfs_producer import produce

"""
## GBFS Producer Backfill DAG

This DAG replays historical GBFS snapshots into Kafka using the provider's
'?at=<timestamp>' parameter.
It does not run on a schedule - you trigger it manually
with start_date and end_date in conf.

Owner: Team Buldo
"""

DEFAULT_ARGS = {
    "owner": "team-buldo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(hours=1),
    "start_date": datetime(2025, 9, 21),
}


def backfill_produce(**context):
    conf = context["dag_run"].conf
    start_date_str = conf.get("start_date")  # e.g. "2025-09-22T17:00:00"
    end_date_str = conf.get("end_date")  # e.g. "2025-09-22T17:00:00"

    if not start_date_str or not end_date_str:
        raise ValueError(
            "Both start_date and end_date must be provided in conf"
        )

    start = datetime.fromisoformat(start_date_str)
    end = datetime.fromisoformat(end_date_str)

    current = start
    while current < end:
        iso_ts = current.strftime("%Y-%m-%dT%H:%M:%SZ")
        produce(iso_ts)
        current += timedelta(minutes=15)

    print(f"Backfill complete from {start} to {end}")


with DAG(
    dag_id="gbfs_backfill_producer_manually",
    default_args=DEFAULT_ARGS,
    description="Backfill historical GBFS messages into Kafka",
    schedule_interval=None,  # only runs when manually triggered
    catchup=False,
    max_active_runs=1,
    tags=["gbfs", "backfill"],
    doc_md=__doc__,
) as dag:
    backfill_task = PythonOperator(
        task_id="backfil_gbfs_messages",
        python_callable=backfill_produce,
        provide_context=True,
    )
