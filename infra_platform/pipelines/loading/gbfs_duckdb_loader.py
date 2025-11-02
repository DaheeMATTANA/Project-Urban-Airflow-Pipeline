import json
from datetime import datetime

import pandas as pd
import pytz
from pipelines.common.duckdb_utils import (
    create_staging_table,
    get_duckdb_connection,
)
from pipelines.common.load_state_utils import (
    get_last_loaded_at,
    update_last_loaded_at,
)
from pipelines.common.minio_utils import get_minio_client

UTC = pytz.utc
CET = pytz.timezone("Europe/Paris")


def load_gbfs_to_duckdb(
    date_str=None,
    hour=None,
    bucket="bronze",
    prefix="gbfs",
    full_refresh=False,
):
    """
    Incrementally load GBFS data from MinIO into DuckDB raw schema.

    Args:
        date_str: Date string (YYYY-MM-DD) to load specific date
        hour: The hour to load specific hour
        bucket: MinIO bucket name
        prefix: Object prefix (gbfs)
        full_refresh: If True, clears table from the date onward & reloads
    """

    # Ensure staging table exists
    create_staging_table()

    # Get clients
    minio_client = get_minio_client()
    duckdb_conn = get_duckdb_connection()

    if full_refresh and date_str:
        print(f"[INFO] Wiping partition {date_str} hour {hour}")
        duckdb_conn.execute(
            f"DELETE FROM raw.raw_gbfs_station_status WHERE ingestion_date = '{date_str}' AND ingestion_hour = {hour}"
        )
        last_loaded_at = None
    else:
        last_loaded_at = get_last_loaded_at(
            duckdb_conn, "raw_gbfs_station_status"
        )

    target_date = date_str or datetime.now(tz=UTC).strftime("%Y-%m-%d")

    print(f"Loading GBFS data for date: {target_date}, hour: {hour}")

    object_prefix = (
        f"{prefix}/station_status/date={target_date}/hour-{hour:02d}/"
    )
    objects = minio_client.list_objects(
        bucket, prefix=object_prefix, recursive=True
    )

    records_loaded = 0
    all_records = []

    for obj in objects:
        if obj.object_name.endswith(".json"):
            try:
                # Get object from MinIO
                response = minio_client.get_object(bucket, obj.object_name)
                data = json.loads(response.read().decode("utf-8"))

                # --- Extract station data ---
                stations = []

                if "payload" in data and "data" in data["payload"]:
                    stations = data["payload"]["data"].get("stations", [])

                elif "data" in data and "stations" in data["data"]:
                    stations = data["data"]["stations"]

                # --- Parse date/hour from file path ---
                path_parts = obj.object_name.split("/")
                date_part = next(
                    (
                        p.split("=")[1]
                        for p in path_parts
                        if p.startswith("date=")
                    ),
                    target_date,
                )
                hour_part = next(
                    (
                        p.split("-")[1]
                        for p in path_parts
                        if p.startswith("hour-")
                    ),
                    "00",
                )

                # --- Prepare records ---
                for station in stations:
                    last_reported_val = station.get("last_reported", 0)
                    last_reported_dt = datetime.fromtimestamp(
                        last_reported_val, tz=UTC
                    )
                    timestamp_cet_cest = last_reported_dt.astimezone(
                        CET
                    ).replace(tzinfo=None)
                    last_reported_naive = last_reported_dt.replace(tzinfo=None)

                    all_records.append(
                        {
                            "station_id": str(station.get("station_id")),
                            "num_bikes_available": station.get(
                                "num_bikes_available"
                            ),
                            "num_docks_available": station.get(
                                "num_docks_available"
                            ),
                            "is_installed": bool(station.get("is_installed")),
                            "is_renting": bool(station.get("is_renting")),
                            "last_reported": last_reported_naive,
                            "timestamp_cet_cest": timestamp_cet_cest,
                            "ingestion_date": date_part,
                            "ingestion_hour": int(hour_part),
                            "file_path": obj.object_name,
                            "loaded_at": datetime.now(tz=UTC),
                            "is_returning": bool(station.get("is_returning")),
                        }
                    )

                response.close()
                response.release_conn()

            except Exception as e:
                print(f"Error processing {obj.object_name}: {e}")
                continue

    # A single bulk insert after loop
    if not all_records:
        print(f"[DEBUG] No records found for {date_str} hour {hour}")
        duckdb_conn.close()
        return 0

    df = pd.DataFrame(all_records)

    df["timestamp_cet_cest"] = pd.to_datetime(
        df["timestamp_cet_cest"], errors="coerce"
    )
    df["ingestion_date"] = pd.to_datetime(
        df["ingestion_date"], errors="coerce"
    ).dt.date

    print(f"[DEBUG] {len(df)} rows before incremental filter")

    cutoff = None
    if last_loaded_at:
        cutoff = last_loaded_at - pd.Timedelta(hours=2)
        df = df[df["last_reported"] > cutoff]
        print(f"[DEBUG] {len(df)} rows after incremental filter > {cutoff}")

    if df.empty:
        print(f"[DEBUG] Skipping insert, no new rows beyond {cutoff}")
        duckdb_conn.close()
        return 0

    # --- Insert into DuckDB ---
    columns_order = [
        "station_id",
        "num_bikes_available",
        "num_docks_available",
        "is_installed",
        "is_renting",
        "last_reported",
        "timestamp_cet_cest",
        "ingestion_date",
        "ingestion_hour",
        "file_path",
        "loaded_at",
        "is_returning",
    ]
    df_ordered = df[columns_order]

    duckdb_conn.register("tmp_df", df_ordered)

    duckdb_conn.execute(
        """
        INSERT INTO raw.raw_gbfs_station_status (
            station_id,
            num_bikes_available,
            num_docks_available,
            is_installed,
            is_renting,
            last_reported,
            timestamp_cet_cest,
            ingestion_date,
            ingestion_hour,
            file_path,
            loaded_at,
            is_returning
        )
        SELECT
            station_id,
            num_bikes_available,
            num_docks_available,
            is_installed,
            is_renting,
            CAST(last_reported AS TIMESTAMP),
            CAST(timestamp_cet_cest AS TIMESTAMP),
            CAST(ingestion_date AS DATE),
            ingestion_hour,
            file_path,
            CAST(loaded_at AS TIMESTAMP),
            is_returning
        FROM tmp_df
        """
    )
    records_loaded = len(df)

    if records_loaded > 0:
        max_ts = duckdb_conn.execute(
            "SELECT MAX(last_reported) FROM raw.raw_gbfs_station_status"
        ).fetchone()[0]
        if max_ts:
            update_last_loaded_at(
                duckdb_conn, "raw.raw_gbfs_station_status", max_ts
            )

    duckdb_conn.commit()
    duckdb_conn.close()

    print(f"Total records loaded: {records_loaded}")
    return records_loaded
