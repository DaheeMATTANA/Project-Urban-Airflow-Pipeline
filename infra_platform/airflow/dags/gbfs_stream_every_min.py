from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.streaming.gbfs_producer import produce

# DAG
default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="gbfs_stream_every_min",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",  # every 1 minute
    catchup=False,
    tags=["gbfs", "streaming"],
) as dag:
    produce_task = PythonOperator(
        task_id="produce_gbfs_station_status",
        python_callable=produce,
    )
