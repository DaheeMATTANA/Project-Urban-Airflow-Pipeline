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

OPEN_METEO_FORECAST_SCHEMA = {
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
        "table_name": "raw.raw_open_meteo",
        "bucket": os.getenv("MINIO_BUCKET", "bronze"),
        "prefix": "openmeteo",
        "schema": OPEN_METEO_SCHEMA,
    }
    return BaseLoader(config)


def get_open_meteo_forecast_loader():
    config = {
        "table_name": "raw.raw_open_meteo_forecast",
        "bucket": os.getenv("MINIO_BUCKET", "bronze"),
        "prefix": "openmeteo_forecast",
        "schema": OPEN_METEO_FORECAST_SCHEMA,
    }
    return BaseLoader(config)
