import os

from pipelines.loading.base_loader import BaseLoader

OPEN_METEO_SCHEMA = {
    "time": "TIMESTAMP",
    "time_cet": "TIMESTAMP",
    "temperature_2m": "DOUBLE",
    "precipitation": "DOUBLE",
    "precipitation_probability": "DOUBLE",
    "visibility": "DOUBLE",
    "windspeed_10m": "DOUBLE",
}


def get_open_meteo_loader():
    config = {
        "table_name": "staging__open_meteo_raw",
        "bucket": os.getenv("MINIO_BUCKET", "raw"),
        "prefix": "openmeteo",
        "schema": OPEN_METEO_SCHEMA,
    }
    return BaseLoader(config)
