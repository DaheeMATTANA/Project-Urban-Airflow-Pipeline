import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


def load_openaq_to_duckdb(**context):
    """
    Task to load OpenAQ to DuckDB using OpenAQLoader class.
    """
    import sys

    sys.path.append("/opt/airflow/src")

    try:
        from pipelines.loading.openaq_duckdb_loader import OpenAQLoader

        loader = OpenAQLoader()
        context["task_instance"].log.info("OpenAQLoader created successfully")

        rows_inserted = loader.load_current_hour()
        context["task_instance"].log.info(
            f"OpenAQ DuckDB loading successful: {rows_inserted} rows"
        )

        summary = loader.get_air_quality_summary()
        context["task_instance"].log.info(
            f"Air quality summary: {len(summary)} parameters loaded"
        )

        return rows_inserted

    except Exception as e:
        context["task_instance"].log.error(
            f"OpenAQ DuckDB loading failed: {e}"
        )
        import traceback

        context["task_instance"].log.error(traceback.format_exc())
        raise


# DAG defaults
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openaq_spark_hourly",
    default_args=default_args,
    description="Ingest hourly OpenAQ data into MinIO via Spark and load to DuckDB",
    schedule_interval="@hourly",  # every hour
    start_date=datetime(2025, 9, 15),
    catchup=False,
    tags=["openaq", "spark", "duckdb"],
) as dag:
    ingest_openaq = SparkSubmitOperator(
        task_id="spark_ingest_openaq",
        application="/opt/airflow/src/pipelines/ingestion/openaq_spark_ingest.py",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        application_args=[],
        name="OpenAQIngest",
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
            "OPENAQ_API_KEY": os.getenv("OPENAQ_API_KEY", ""),
            "MINIO_BUCKET": "raw",
            "MINIO_ENDPOINT": "minio:9000",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
        },
    )

    load_to_duckdb_task = PythonOperator(
        task_id="load_openaq_to_duckdb",
        python_callable=load_openaq_to_duckdb,
        provide_context=True,
    )

    ingest_openaq >> load_to_duckdb_task
