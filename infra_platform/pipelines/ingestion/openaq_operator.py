import datetime
import io
import json
import os

import requests
from dotenv import load_dotenv
from minio import Minio

# Load environment
load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError("OPENAQ_API_KEY missing in .env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "raw"

API_BASE = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": API_KEY}

# Paris bounding box
PARIS_BBOX = [2.2241, 48.8156, 2.4699, 48.9021]
PARIS_BBOX_STR = ",".join(map(str, PARIS_BBOX))

# MinIO Client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


def ensure_bucket(bucket_name: str):
    """
    Create bucket if it doesn't exist.
    """
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
        print(f"Created bucket {bucket_name}")
    else:
        print(f"Bucket {bucket_name} already exists")


def find_locations_paris(limit=10):
    """
    Find monitoring locations in Paris (bbox).
    """
    resp = requests.get(
        f"{API_BASE}/locations",
        headers=HEADERS,
        params={"bbox": PARIS_BBOX_STR, "limit": limit},
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        print("No locations found in Paris bbox.")
    return results


def fetch_sensor_data(sensor_id: int, limit: int = 100):
    """
    Fetch hourly data for a sensor.
    """
    resp = requests.get(
        f"{API_BASE}/sensors/{sensor_id}/hours",
        headers=HEADERS,
        params={"limit": limit},
    )
    resp.raise_for_status()
    return resp.json()


def save_to_minio(data: dict, sensor_id: int):
    """
    Save JSON data directly into MinIO with partitioned path.
    """
    now = datetime.datetime.utcnow()
    object_path = f"openaq/yyyy={now:%Y}/MM={now:%m}/dd={now:%d}/hh={now:%H}/sensor_{sensor_id}.json"

    # Convert dict to bytes
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    stream = io.BytesIO(payload)

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_path,
        data=stream,
        length=len(payload),
        content_type="application/json",
    )

    print(
        f"Uploaded {len(data.get('results', []))} records -> s3://{MINIO_BUCKET}/{object_path}"
    )
    return object_path


def ingest_paris(limit_locations=3, limit_records=100):
    """
    Ingest all Paris sensors and save them to MinIO.
    """
    ensure_bucket(MINIO_BUCKET)

    locations = find_locations_paris(limit=limit_locations)
    saved_objects = []

    for loc in locations:
        print(f"--- Location: {loc['name']} ---")
        for sensor in loc.get("sensors", []):
            sid = sensor["id"]
            pname = sensor["parameter"]["name"]
            print(f"Fetching {pname} from sensor {sid} ...")
            data = fetch_sensor_data(sid, limit=limit_records)
            saved_objects.append(save_to_minio(data, sid))
    return saved_objects


if __name__ == "__main__":
    ingest_paris(limit_locations=2, limit_records=10)
