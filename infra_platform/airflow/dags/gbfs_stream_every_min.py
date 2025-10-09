from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pipelines.streaming.gbfs_producer import produce

"""
## GBFS message producer DAG

This DAG produces **GBFS messages** every minutes (near real-time) :
- Produces GBFS messages from provider API
- Writes into 'gbfs_station_status' topic on Redpanda
- Pushes metadata to DuckDB staging

Owner : Team Buldo
"""

# DAG
default_args = {
    "owner": "team-buldo",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "start_date": days_ago(1),
}

with DAG(
    dag_id="gbfs_stream_every_min",
    default_args=default_args,
    start_date=datetime(2025, 9, 15),
    schedule_interval="* * * * *",  # At every minute
    catchup=False,
    tags=["produce:gbfs"],
    doc_md=__doc__,
) as dag:
    produce_task = PythonOperator(
        task_id="produce_gbfs_station_status",
        python_callable=produce,
        op_kwargs={"date_filter": "{{ ds }}"},
    )
