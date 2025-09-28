import os

from dotenv import load_dotenv
from pipelines.common.http_utils import fetch_json_with_retry
from pipelines.common.minio_utils import (
    write_to_minio_hourly,
)

load_dotenv()

MINIO_BUCKET = "bronze"

URL = "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2"


# Fetch data
def fetch_and_store_idfm_data(**context):
    API_TOKEN = os.getenv("IDFM_API_KEY")
    if not API_TOKEN:
        raise ValueError("IDFM_API_KEY missing in .env")
    headers = {"apiKey": API_TOKEN}

    resp = fetch_json_with_retry(url=URL, headers=headers)

    date_str = context["ds"]
    hour = context["logical_date"].hour

    write_to_minio_hourly(
        bucket=MINIO_BUCKET,
        prefix="idfm_disruption",
        message=resp,
        event_key="disruptions",
        date_str=date_str,
        hour=hour,
    )


if __name__ == "__main__":
    fetch_and_store_idfm_data()
