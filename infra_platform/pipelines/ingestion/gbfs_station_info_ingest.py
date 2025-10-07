import datetime
import json
from io import BytesIO

from dotenv import load_dotenv
from pipelines.common.http_utils import fetch_json_with_retry
from pipelines.common.minio_utils import get_minio_client

# Load environment
load_dotenv()

URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"


# Data
def write_to_minio_monthly(
    bucket: str, prefix: str, data: dict, filename: str = None
):
    """
    Write to MinIO bucket station_information.json partitioned by month.
    """
    now_utc = datetime.datetime.now(datetime.UTC)
    date_str = now_utc.strftime("%Y-%m-01")
    filename = filename or f"{date_str}.json"
    path = f"{prefix}/station_information/date={date_str}/{filename}"

    client = get_minio_client()

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    bio = BytesIO(json.dumps(data).encode("utf-8"))
    count_bytes = len(bio.getvalue())

    client.put_object(
        bucket_name=bucket,
        object_name=path,
        data=bio,
        length=count_bytes,
        content_type="application/json",
    )

    print(f"Uploaded {count_bytes}B to s3://{bucket}/{path}")


def run_ingest():
    resp = fetch_json_with_retry(url=URL)
    write_to_minio_monthly(
        bucket="bronze",
        prefix="gbfs",
        data=resp,
        filename="station_information.json",
    )


if __name__ == "__main__":
    run_ingest()
