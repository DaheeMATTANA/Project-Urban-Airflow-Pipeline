import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from dags.common.defaults import DEFAULT_ARGS
from pipelines.loading.openaq_duckdb_loader import get_openaq_loader

"""
## Open Air Quality Spark Batch Ingestion DAG

This DAG ingests **OpenAQ data** every hour :
- Fetches air quality data from provider API
- Stores raw data in MinIO (bronze)
- Pushes metadata to DuckDB staging

* Important parameter : full_refresh = False will run incremental loads
* and full_refresh needs to be implemented only when it is absolutely necessary.

Owner : Team Buldo
"""


def load_openaq_to_duckdb(**context):
    conf = context["dag_run"].conf or {}
    full_refresh = conf.get("full_refresh", False)
    date_str = context["ds"]
    hour = context["logical_date"].hour
    loader = get_openaq_loader()
    count = loader.load_partition(
        date_str=date_str, hour=hour, full_refresh=full_refresh
    )

    context["task_instance"].log.info(
        f"[INFO] Inserted {count} rows into {loader.table_name} for {date_str} hour {hour} (full_refresh={full_refresh})"
    )
    return count


with DAG(
    dag_id="openaq_hourly",
    default_args=DEFAULT_ARGS,
    description="Ingest hourly OpenAQ data into MinIO (bronze) + DuckDB",
    schedule_interval="@hourly",  # every hour
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=1,
    tags=["source:openaq"],
    doc_md=__doc__,
) as dag:
    ingest_openaq = SparkSubmitOperator(
        task_id="spark_ingest_openaq",
        application="/opt/airflow/src/pipelines/ingestion/openaq_spark_ingest.py",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        application_args=["{{ ds }}", "{{ logical_date.hour }}"],
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
            "MINIO_BUCKET": "bronze",
            "MINIO_ENDPOINT": "minio:9000",
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
        },
    )

    load_to_duckdb_task = PythonOperator(
        task_id="load_openaq_to_duckdb",
        python_callable=load_openaq_to_duckdb,
        provide_context=True,
        op_kwargs={
            "date_filter": "{{ ds }}"  # Use Airflow execution date
        },
    )

    ingest_openaq >> load_to_duckdb_task
