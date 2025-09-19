from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.common.defaults import DEFAULT_ARGS
from pipelines.ingestion.gbfs_consumer_batch import consume_batch
from pipelines.loading.gbfs_duckdb_loader import load_gbfs_to_duckdb

"""
## GBFS Stream Ingestion DAG

This DAG ingests **GBFS bike sharing data** every 5 minutes (near real-time) :
- Fetches live station status from provider API
- Stores raw data in MinIO (bronze)
- Pushes metadata to DuckDB staging

Owner : Team Buldo
"""

with DAG(
    dag_id="gbfs_consumer_5_min",
    default_args=DEFAULT_ARGS,
    description="Ingest every 5 minutes GBFS data into MinIO (bronze) + DuckDB",
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["source:gbfs"],
    doc_md=__doc__,
) as dag:
    consume_task = PythonOperator(
        task_id="consume_gbfs_to_minio_batch",
        python_callable=consume_batch,
        op_kwargs={
            "max_messages": 500,
            "timeout_s": 30,
        },
    )

    # Optional loader task - can be enabled/disabled
    load_to_duckdb_task = PythonOperator(
        task_id="load_gbfs_to_duckdb",
        python_callable=load_gbfs_to_duckdb,
    )

    consume_task >> load_to_duckdb_task
