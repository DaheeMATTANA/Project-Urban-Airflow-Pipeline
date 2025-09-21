import os
import sys
from datetime import datetime

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_utc_timestamp, to_timestamp

# Load .env
load_dotenv()

# MinIO configs
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def fetch_and_store_open_meteo(exec_date: str, exec_hour: str):
    dt = datetime.fromisoformat(
        f"{exec_date}T{exec_hour.zfill(2)}:00:00+00:00"
    )

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
        "timezone": "UTC",
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

    # Hourly filter
    df_hourly = df.filter(
        (col("time").cast("date") == exec_date)
        & (col("time").substr(12, 2) == exec_hour.zfill(2))
    )

    # Partition path
    partition_path = dt.strftime("yyyy=%Y/mm=%m/dd=%d/hh=%H")
    base_path = f"s3a://{MINIO_BUCKET}/openmeteo/{partition_path}/"

    # Save to MinIO as Parquet
    df_hourly.write.mode("overwrite").parquet(base_path)
    print(f"[HOURLY] Wrote {df_hourly.count()} rows to {base_path}")

    # Seperate forecasts daily
    forecast_path = dt.strftime("yyyy=%Y/mm=%m/dd=%d")
    base_forecast_path = (
        f"s3a://{MINIO_BUCKET}/openmeteo_forecast/{forecast_path}/"
    )

    df.write.mode("overwrite").parquet(base_forecast_path)
    print(f"[FORECAST] Wrote {df.count()} rows to {base_forecast_path}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise ValueError(
            "Usage: openmeteo_spark_ingest.py <exec_date:YYYY-MM-DD> <hour:HH>"
        )
    fetch_and_store_open_meteo(sys.argv[1], sys.argv[2])
