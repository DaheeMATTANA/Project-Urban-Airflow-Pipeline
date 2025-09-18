import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from pipelines.loading.open_meteo_duckdb_loader import get_open_meteo_loader


def load_partition_to_duckdb(**context):
    logical_date = context["logical_date"]
    date_str = logical_date.strftime("%Y-%m-%d")
    hour = logical_date.hour
    partition_path = logical_date.strftime("yyyy=%Y/mm=%m/dd=%d/hh=%H")
    s3_path = f"s3a://raw/openmeteo/{partition_path}/*.parquet"

    loader = get_open_meteo_loader()
    count = loader.load_data(s3_path, date_str, hour)

    print(f"Inserted {count} rows into {loader.table_name}")


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK_APP = "/opt/airflow/src/pipelines/ingestion/open_meteo_spark_ingest.py"

with DAG(
    dag_id="open_meteo_hourly",
    description="Ingest hourly Open-Meteo weather data into MinIO + DuckDB",
    schedule_interval="@hourly",
    start_date=datetime(2025, 9, 15),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["weather", "openmeteo", "spark"],
) as dag:
    ingest_task = SparkSubmitOperator(
        task_id="open_meteo_spark_ingest",
        application=SPARK_APP,
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        application_args=["{{ ds }}T{{ execution_date.hour }}:00:00"],
        name="OpenMeteoIngest",
        conf={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.hadoop.fs.s3a.endpoint": os.getenv(
                "MINIO_ENDPOINT", "http://localhost:9000"
            ),
            "spark.hadoop.fs.s3a.access.key": os.getenv(
                "MINIO_ACCESS_KEY", "minioadmin"
            ),
            "spark.hadoop.fs.s3a.secret.key": os.getenv(
                "MINIO_SECRET_KEY", "minioadmin"
            ),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        env_vars={
            "MINIO_BUCKET": "raw",
            "MINIO_ENDPOINT": "minio:9000",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
        },
    )

    duckdb_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_partition_to_duckdb,
        provide_context=True,
    )

    ingest_task >> duckdb_task
