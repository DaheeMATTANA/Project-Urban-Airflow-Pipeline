import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

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
    description="Ingest hourly OpenAQ data into MinIO via Spark",
    schedule_interval="@hourly",  # every hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["openaq", "spark"],
) as dag:
    ingest_openaq = SparkSubmitOperator(
        task_id="spark_ingest_openaq",
        application="/opt/airflow/src/pipelines/ingestion/openaq_spark_ingest.py",
        conn_id="spark_default",
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
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
        },
    )
