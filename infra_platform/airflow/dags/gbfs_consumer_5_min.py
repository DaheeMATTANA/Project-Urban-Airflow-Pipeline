from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.ingestion.gbfs_consumer_batch import consume_batch
from pipelines.loading.gbfs_duckdb_loader import load_gbfs_to_duckdb

with DAG(
    dag_id="gbfs_consumer_5_min",
    start_date=datetime(2025, 9, 15),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    consume_task = PythonOperator(
        task_id="consume_gbfs_to_minio_batch",
        python_callable=consume_batch,
        op_kwargs={
            "max_messages": 100,
            "timeout_s": 30,
        },
    )

    # Optional loader task - can be enabled/disabled
    load_to_duckdb_task = PythonOperator(
        task_id="load_gbfs_to_duckdb",
        python_callable=load_gbfs_to_duckdb,
        op_kwargs={
            "date_filter": "{{ ds }}"  # Use Airflow execution date
        },
    )

    consume_task >> load_to_duckdb_task
