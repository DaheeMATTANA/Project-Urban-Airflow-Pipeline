from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.ingestion.gbfs_consumer_batch import consume_batch

with DAG(
    dag_id="gbfs_consumer_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="consume_gbfs_to_minio_batch",
        python_callable=consume_batch,
        op_kwargs={
            "max_messages": 100,
            "timeout_s": 30,
        },
    )
