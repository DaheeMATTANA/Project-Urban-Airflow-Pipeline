import datetime
import json
import os
from io import BytesIO

from minio import Minio


def get_minio_client():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False,
    )
    return client


def persist_message_to_minio(bucket: str, prefix: str, message: dict):
    """
    Write message to MinIO under partitioned path:
    bronze/gbfs/station_status/date=YYYY-MM-DD/hour=HH/file.json
    """
    client = get_minio_client()

    # Ensure bucket exists
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    now_utc = datetime.datetime.now(datetime.UTC)

    date = now_utc.strftime("%Y-%m-%d")
    hour = now_utc.strftime("%H")

    path = (
        f"{prefix}/station_status/date={date}/hour-{hour}/"
        f"{now_utc.isoformat()}.json"
    )

    # Convert to JSON
    data_bytes = BytesIO(json.dumps(message).encode("utf-8"))

    # Upload
    client.put_object(
        bucket_name=bucket,
        object_name=path,
        data=data_bytes,
        length=len(data_bytes.getvalue()),
        content_type="application/json",
    )

    print(f"Uploaded to s3://{bucket}/{path}")
