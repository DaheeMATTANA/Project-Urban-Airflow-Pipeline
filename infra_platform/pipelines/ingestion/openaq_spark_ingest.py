import datetime
import os

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Load .env
load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError("OPENAQ_API_KEY missing in .env")

API_BASE = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}

# Paris bbox
PARIS_BBOX = [2.2241, 48.8156, 2.4699, 48.9021]
PARIS_BBOX_STR = ",".join(map(str, PARIS_BBOX))

# MinIO configs
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# SparkSesssion with S3(MinIO) connector
spark = SparkSession.builder.appName("OpenAQSparkIngestion").getOrCreate()

# Spark Schema
schema = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("parameter", StringType(), True),
        StructField("location", StringType(), True),
    ]
)


def get_paris_sensors(limit=10):
    """
    Return a list of sensor IDs for Paris bbox.
    """
    resp = requests.get(
        f"{API_BASE}/locations",
        headers=HEADERS,
        params={"bbox": PARIS_BBOX_STR, "limit": limit},
    )
    resp.raise_for_status()
    locations = resp.json()["results"]
    sensor_ids = []
    for loc in locations:
        for sensor in loc.get("sensors", []):
            sensor_ids.append(
                {
                    "sensor_id": str(sensor["id"]),
                    "parameter": sensor["parameter"]["name"],
                    "location": loc["name"],
                }
            )
    return sensor_ids


def fetch_sensor_data(sensor_info):
    """
    Fetch hourly data for one sensor - VERSION FINALE QUI MARCHE
    """
    sid = sensor_info["sensor_id"]
    pname = sensor_info["parameter"]
    loc_name = sensor_info["location"]

    print(f"Fetching data for sensor {sid} ({pname}) at {loc_name}")

    try:
        resp = requests.get(
            f"{API_BASE}/sensors/{sid}/hours",
            headers=HEADERS,
            params={"limit": 100},
            timeout=30,
        )

        if resp.status_code != 200:
            print(f"API Error {resp.status_code} for sensor {sid}")
            return []

        data = resp.json()
        results = data.get("results", [])
        print(f"Found {len(results)} measurements for sensor {sid}")

        enriched = []
        for r in results:
            period = r.get("period", {})
            datetime_from = period.get("datetimeFrom", {})
            datetime_str = datetime_from.get("utc")

            value = r.get("value")
            parameter_info = r.get("parameter", {})
            unit = parameter_info.get("units")  # Attention: "units" pas "unit"

            if datetime_str is None or value is None:
                continue

            try:
                dt = datetime.datetime.fromisoformat(
                    datetime_str.replace("Z", "+00:00")
                )

                enriched.append(
                    {
                        "timestamp": dt,
                        "value": float(value),
                        "unit": unit,
                        "sensor_id": sid,
                        "parameter": pname,
                        "location": loc_name,
                    }
                )

            except Exception as e:
                print(f"Parsing error: {e}")
                continue

        print(
            f"Successfully processed {len(enriched)} records for sensor {sid}"
        )
        return enriched

    except Exception as e:
        print(f"Error for sensor {sid}: {e}")
        return []


if __name__ == "__main__":
    # Get sensors in Paris
    sensors = get_paris_sensors(limit=5)  # Limit for demo
    print(f"Found {len(sensors)} sensors in Paris")

    # Distribute ingestion with Spark
    rdd = spark.sparkContext.parallelize(sensors)
    data = rdd.flatMap(fetch_sensor_data).collect()

    if not data:
        print("No data fetched")
    else:
        # Convert to DataFrame
        df = spark.createDataFrame(data)

        # Write to MinIO in partitioned Parquet
        now = datetime.datetime.now(datetime.UTC)
        path = (
            f"s3a://{MINIO_BUCKET}/openaq/"
            f"yyyy={now:%Y}/MM={now:%m}/dd={now:%d}/hh={now:%H}/"
        )
        df.write.mode("overwrite").parquet(path)

        print(f"Wrote {df.count()} rows to {path}")
