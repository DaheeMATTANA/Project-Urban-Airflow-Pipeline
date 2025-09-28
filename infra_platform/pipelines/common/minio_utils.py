import datetime
import json
import os
from io import BytesIO

from minio import Minio


def get_minio_client():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )
    return client


def write_to_minio_hourly(
    bucket: str,
    prefix: str,
    message: dict,
    event_key: str = None,
    date_str: str = None,
    hour: int = None,
):
    """
    Write message to MinIO under partitioned path by hour.
    If date_str and hour are not provided, fallback to current UTC time.
    """
    client = get_minio_client()

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    if date_str is None or hour is None:
        now_utc = datetime.datetime.now(datetime.UTC)
        date_str = now_utc.strftime("%Y-%m-%d")
        hour = now_utc.strftime("%H")
    else:
        hour = f"{int(hour):02d}"

    filename = datetime.datetime.now(datetime.UTC).isoformat()
    path = f"{prefix}/date={date_str}/hour-{hour}/{filename}.json"

    data_bytes = BytesIO(json.dumps(message).encode("utf-8"))
    event_count = len(message.get(event_key, []))
    count_bytes = len(data_bytes.getvalue())

    client.put_object(
        bucket_name=bucket,
        object_name=path,
        data=data_bytes,
        length=count_bytes,
        content_type="application/json",
    )

    if event_count is not None:
        print(
            f"Uploaded {event_count} {event_key} events to s3://{bucket}/{path}, {count_bytes}B"
        )
    else:
        print(f"Uploaded {count_bytes}B to s3://{bucket}/{path}")


def persist_message_to_minio(bucket: str, prefix: str, message: dict):
    """
    ** GBFS Only.
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
