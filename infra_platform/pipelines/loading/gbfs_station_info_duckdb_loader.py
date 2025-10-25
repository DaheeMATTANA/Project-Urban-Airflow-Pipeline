import datetime
import hashlib
import json

from pipelines.loading.json_loader import JsonLoader

STATION_INFO_SCHEMA = {
    "station_id": "VARCHAR",
    "stationCode": "VARCHAR",
    "name": "VARCHAR",
    "lat": "DOUBLE",
    "lon": "DOUBLE",
    "capacity": "INTEGER",
    "station_opening_hours": "VARCHAR",
    "rental_methods": "VARCHAR",
    "record_hash": "VARCHAR",
}


def compute_record_hash(station: dict) -> str:
    """
    Compute a deterministic md5 hash of station content.
    """
    exclude = {"ingestion_date", "ingestion_hour"}
    filtered = {k: v for k, v in station.items() if k not in exclude}
    payload = json.dumps(filtered, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def flatten_gbfs_station_info(data: dict, date_str: str, hour: int):
    """
    Extracts the list of stations from GBFS station_information.json.
    """
    stations = (
        data.get("data", {}).get("stations") or data.get("stations") or []
    )

    rows = []
    for station in stations:
        row = station.copy()
        row["ingestion_date"] = date_str
        row["ingestion_hour"] = 0
        row["record_hash"] = compute_record_hash(row)
        rows.append(row)

    print(f"[DEBUG] Flattened {len(rows)} stations")
    return rows


class StationInfoLoader(JsonLoader):
    """
    Loader adapted for monthly station_information files.
    """

    def build_s3_path(self, date_str, hour=None):
        return f"s3a://{self.bucket}/{self.prefix}/date={date_str}/*.json"

    def get_minio_prefix(self, date_str, hour):
        return f"{self.prefix}/date={date_str}/"


def get_station_info_loader():
    config = {
        "table_name": "raw.raw_gbfs_station_information",
        "bucket": "bronze",
        "prefix": "gbfs/station_information",
        "schema": STATION_INFO_SCHEMA,
    }
    loader = StationInfoLoader(config)
    loader.flatten_records = flatten_gbfs_station_info
    return loader


def run_station_info_load(**context):
    loader = get_station_info_loader()

    now_utc = datetime.datetime.now(datetime.UTC)
    date_str = now_utc.strftime("%Y-%m-01")

    s3_path = loader.build_s3_path(date_str)
    count = loader.load_data(
        s3_path=s3_path, date_str=date_str, hour=0, full_refresh=False
    )

    print(f"Loaded {count} rows into {loader.table_name} for {date_str}")


if __name__ == "__main__":
    run_station_info_load()
