import os

from pipelines.loading.base_loader import BaseLoader

OPENAQ_SCHEMA = {
    "timestamp": "TIMESTAMP",
    "timestamp_cet": "TIMESTAMP",
    "value": "DOUBLE",
    "unit": "VARCHAR",
    "sensor_id": "VARCHAR",
    "parameter": "VARCHAR",
    "location": "VARCHAR",
}


def get_openaq_loader():
    config = {
        "table_name": "staging__openaq_raw",
        "bucket": os.getenv("MINIO_BUCKET", "bronze"),
        "prefix": "openaq",
        "schema": OPENAQ_SCHEMA,
    }
    return BaseLoader(config)
