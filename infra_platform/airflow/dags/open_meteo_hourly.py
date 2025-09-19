import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from dags.common.defaults import DEFAULT_ARGS
from pipelines.loading.open_meteo_duckdb_loader import get_open_meteo_loader

"""
## Open Meteo Spark Batch Ingestion DAG

This DAG ingests **Open Meteo data** every hour :
- Fetches weather conditions data from provider API
- Stores raw data in MinIO (bronze)
- Pushes metadata to DuckDB staging

Owner : Team Buldo
"""


def load_openmeteo_to_duckdb(**context):
    date_str = context["ds"]
    hour = context["logical_date"].hour
    loader = get_open_meteo_loader()
    count = loader.load_partition(date_str, hour)

    context["task_instance"].log.info(
        f"Inserted {count} rows into {loader.table_name} for {date_str} hour {hour}"
    )
    return count


SPARK_APP = "/opt/airflow/src/pipelines/ingestion/open_meteo_spark_ingest.py"

with DAG(
    dag_id="open_meteo_hourly",
    default_args=DEFAULT_ARGS,
    description="Ingest hourly Open-Meteo weather data into MinIO (bronze) + DuckDB",
    schedule_interval="@hourly",
    max_active_runs=1,
    tags=["source:openmeteo"],
    doc_md=__doc__,
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
            "MINIO_BUCKET": "bronze",
            "MINIO_ENDPOINT": "minio:9000",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
        },
    )

    duckdb_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_openmeteo_to_duckdb,
        provide_context=True,
        op_kwargs={
            "date_filter": "{{ ds }}"  # Use Airflow execution date
        },
    )

    ingest_task >> duckdb_task
