import os
from datetime import UTC, datetime

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_utc_timestamp, to_timestamp

# Load .env
load_dotenv()

# MinIO configs
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def fetch_and_store_open_meteo(execution_date, **context):
    if execution_date is None:
        execution_date = datetime.datetime.now(datetime.UTC)
    # Spark session
    spark = SparkSession.builder.appName("OpenMeteoIngestion").getOrCreate()

    # API request
    latitude, longitude = 48.8566, 2.3522

    hourly_params = [
        "temperature_2m",
        "precipitation",
        "precipitation_probability",
        "visibility",
        "windspeed_10m",
    ]

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(hourly_params),
        "timezone": "Europe/Paris",
    }

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    # Flatten JSON -> rows
    hourly = data["hourly"]
    rows = list(
        zip(
            hourly["time"],
            hourly["temperature_2m"],
            hourly["precipitation"],
            hourly["precipitation_probability"],
            hourly["visibility"],
            hourly["windspeed_10m"],
            strict=False,
        )
    )
    df = spark.createDataFrame(
        rows,
        [
            "time",
            "temperature_2m",
            "precipitation",
            "precipitation_probability",
            "visibility",
            "windspeed_10m",
        ],
    )

    df = df.withColumn("time", to_timestamp(col("time")))

    # Add Paris time
    df = df.withColumn(
        "time_cet", from_utc_timestamp(col("time"), "Europe/Paris")
    )

    # Partition path
    partition_path = execution_date.strftime("yyyy=%Y/mm=%m/dd=%d/hh=%H")
    base_path = f"s3a://{MINIO_BUCKET}/openmeteo/{partition_path}/"

    # Save to MinIO as Parquet
    (df.write.mode("overwrite").parquet(base_path))


if __name__ == "__main__":
    fetch_and_store_open_meteo(datetime.now(UTC))
