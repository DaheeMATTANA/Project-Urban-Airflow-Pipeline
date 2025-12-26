import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

"""
## Warehouse_dev snapshop upload DAG

This DAG runs every day at 2am to upload warehouse_dev snapshot
into MinIO 'snapshots' bucket
to be usable for GitHub action CI to build dataset in preprod or prod.

Owner: Team Buldo
"""

# Config
DUCKDB_PATH = "/opt/airflow/data/warehouse_dev.duckdb"
MINIO_BUCKET = "snapshots"
MINIO_KEY = "warehouse_dev.duckdb"


def upload_duckdb_to_minio():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )
    client.fput_object(
        bucket_name=MINIO_BUCKET,
        object_name=MINIO_KEY,
        file_path=DUCKDB_PATH,
    )
    print(f"Uploaded {DUCKDB_PATH} to {MINIO_BUCKET}/{MINIO_KEY}")


with DAG(
    dag_id="snapshot_warehouse_dev_daily",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    description="Snapshot dev DuckDB file into MinIO for CI/CD",
    schedule_interval="0 2 * * *",  # At 02:00
    start_date=datetime(2025, 9, 29),
    tags=["snapshot_db"],
    doc_md=__doc__,
    catchup=False,
) as dag:
    upload_task = PythonOperator(
        task_id="upload_duckdb",
        python_callable=upload_duckdb_to_minio,
    )
