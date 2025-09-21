import datetime
import os
import sys

import pytz
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
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# SparkSesssion with S3(MinIO) connector
spark = SparkSession.builder.appName("OpenAQSparkIngestion").getOrCreate()

# Spark Schema
schema = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("timestamp_cet", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("parameter", StringType(), True),
        StructField("location", StringType(), True),
    ]
)


def get_paris_sensors(limit=100, days_active=7):
    """
    Return a list of active sensor IDs for Paris bbox,
    filtering out sensors that haven't reported in the last N days.
    Always returns a list (possibly empty).
    """
    import datetime

    try:
        resp = requests.get(
            f"{API_BASE}/locations",
            headers=HEADERS,
            params={
                "bbox": PARIS_BBOX_STR,
                "limit": limit,
                "sort": "desc",
                "include": "latest",
            },
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"API error while fetching locations: {e}")
        return []

    locations = resp.json().get("results", [])
    sensor_ids = []

    cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
        days=days_active
    )

    for loc in locations:
        # some locations don’t have lastUpdated
        last_updated = loc.get("lastUpdated")
        dt_last = None
        if last_updated:
            try:
                dt_last = datetime.datetime.fromisoformat(
                    last_updated.replace("Z", "+00:00")
                )
            except Exception:
                pass

        # skip inactive locations if we could parse lastUpdated
        if dt_last and dt_last < cutoff:
            continue

        for sensor in loc.get("sensors", []):
            sensor_ids.append(
                {
                    "sensor_id": str(sensor.get("id")),
                    "parameter": sensor.get("parameter", {}).get("name", ""),
                    "location": loc.get("name", "Unknown"),
                }
            )

    print(
        f"Found {len(sensor_ids)} active sensors in Paris (last {days_active} days)"
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

    utc_now = datetime.datetime.now(datetime.UTC)
    date_from = (utc_now - datetime.timedelta(hours=1)).isoformat()

    try:
        resp = requests.get(
            f"{API_BASE}/sensors/{sid}/hours",
            headers=HEADERS,
            params={"limit": 1000, "date_from": date_from},
            timeout=30,
        )

        if resp.status_code != 200:
            print(f"API Error {resp.status_code} for sensor {sid}")
            return []

        data = resp.json()
        results = data.get("results", [])
        print(f"Found {len(results)} measurements for sensor {sid}")

        enriched = []
        paris_tz = pytz.timezone("Europe/Paris")

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
                local_dt = dt.astimezone(paris_tz).replace(tzinfo=None)

                enriched.append(
                    {
                        "timestamp": dt.replace(tzinfo=None),
                        "timestamp_cet": local_dt,
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
    if len(sys.argv) < 3:
        raise ValueError(
            "Usage: openaq_spark_ingest.py <exec_date:YYYY-MM-DD> <hour:HH>"
        )

    exec_date = sys.argv[1]
    exec_hour = sys.argv[2]

    dt = datetime.datetime.fromisoformat(
        f"{exec_date}T{exec_hour.zfill(2)}:00:00+00:00"
    )

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
        df = spark.createDataFrame(data, schema=schema)

        # Write to MinIO in partitioned Parquet
        path = (
            f"s3a://{MINIO_BUCKET}/openaq/"
            f"yyyy={dt:%Y}/mm={dt:%m}/dd={dt:%d}/hh={dt:%H}/"
        )
        df.write.mode("overwrite").parquet(path)

        print(f"Wrote {df.count()} rows to {path}")
