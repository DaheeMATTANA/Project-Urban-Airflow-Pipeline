import json
import os
from io import BytesIO

import requests
from dotenv import load_dotenv
from minio import Minio

# Load environment
load_dotenv()
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bronze"

API_BASE = "https://calendrier.api.gouv.fr/jours-feries/metropole.json"

# MinIO Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def ensure_bucket(bucket_name: str):
    """
    Create a bucket if it does not exist.
    """
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Created bucket {bucket_name}")


def fetch_holidays_data() -> dict:
    """
    Fetch holidays data with API call.
    """
    resp = requests.get(API_BASE)
    resp.raise_for_status()
    return resp.json()


def write_to_minio_yearly(data: dict, min_year: int):
    """
    Write to MinIO bucket holidays.json partitioned by year
    """
    ensure_bucket(MINIO_BUCKET)

    # Partition by year
    years = {}
    for date, name in data.items():
        year = int(date.split("-")[0])
        if year < min_year:
            continue
        years.setdefault(year, {})[date] = name

    for year, holidays in years.items():
        object_path = f"holidays_fr/year={year}/holidays.json"
        bio = BytesIO(json.dumps(holidays, ensure_ascii=False).encode("utf-8"))

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_path,
            data=bio,
            length=len(bio.getvalue()),
            content_type="application/json",
        )
        print(f"Stored holidays for {year} at {object_path}")


def run_ingest(min_year: int):
    data = fetch_holidays_data()
    write_to_minio_yearly(data, min_year=min_year)


if __name__ == "__main__":
    run_ingest(min_year=2025)
