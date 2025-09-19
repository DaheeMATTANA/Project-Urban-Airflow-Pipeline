import json
from datetime import datetime, timedelta

import pandas as pd
from pipelines.common.duckdb_utils import (
    create_staging_table,
    get_duckdb_connection,
)
from pipelines.common.minio_utils import get_minio_client


def load_gbfs_to_duckdb(bucket="bronze", prefix="gbfs", date_filter=None):
    """
    Load GBFS data from MinIO to DuckDB staging table

    Args:
        bucket: MinIO bucket name
        prefix: Object prefix (gbfs)
        date_filter: Optional date string (YYYY-MM-DD) to load specific date
    """

    # Ensure staging table exists
    create_staging_table()

    # Get clients
    minio_client = get_minio_client()
    duckdb_conn = get_duckdb_connection()

    # Determine date to process
    if date_filter:
        target_date = date_filter
    else:
        # Default to yesterday to ensure complete data
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Loading GBFS data for date: {target_date}")

    # List objects for the target date
    object_prefix = f"{prefix}/station_status/date={target_date}/"
    objects = minio_client.list_objects(
        bucket, prefix=object_prefix, recursive=True
    )

    records_loaded = 0

    for obj in objects:
        if obj.object_name.endswith(".json"):
            try:
                # Get object from MinIO
                response = minio_client.get_object(bucket, obj.object_name)
                data = json.loads(response.read().decode("utf-8"))

                # --- Extract station data ---
                stations = []
                timestamp_cet = None

                if "payload" in data and "data" in data["payload"]:
                    stations = data["payload"]["data"].get("stations", [])
                    timestamp_cet = data.get("timestamp_cet_cest")

                elif "data" in data and "stations" in data["data"]:
                    stations = data["data"]["stations"]
                    if "last_updated" in data:
                        timestamp_cet = datetime.fromtimestamp(
                            data["last_updated"]
                        ).isoformat()

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

                # --- Prepare records for bulk insert ---
                records = []
                for station in stations:
                    record = {
                        "station_id": str(station.get("station_id")),
                        "num_bikes_available": station.get(
                            "num_bikes_available"
                        ),
                        "num_docks_available": station.get(
                            "num_docks_available"
                        ),
                        "is_installed": bool(station.get("is_installed")),
                        "is_renting": bool(station.get("is_renting")),
                        "last_reported": datetime.fromtimestamp(
                            station.get("last_reported", 0)
                        ),
                        "timestamp_cet_cest": timestamp_cet,
                        "ingestion_date": date_part,
                        "ingestion_hour": int(hour_part),
                        "file_path": obj.object_name,
                        "loaded_at": datetime.now(),
                        "is_returning": bool(station.get("is_returning")),
                    }
                    records.append(record)

                # --- Insert into DuckDB ---
                if records:
                    df = pd.DataFrame(records)

                    df["timestamp_cet_cest"] = pd.to_datetime(
                        df["timestamp_cet_cest"], errors="coerce"
                    )
                    df["ingestion_date"] = pd.to_datetime(
                        df["ingestion_date"], errors="coerce"
                    ).dt.date

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
                    records_loaded += len(records)
                    print(
                        f"Loaded {len(records)} records from {obj.object_name}"
                    )

                response.close()
                response.release_conn()

            except Exception as e:
                print(f"Error processing {obj.object_name}: {e}")
                continue

    duckdb_conn.commit()
    duckdb_conn.close()

    print(f"Total records loaded: {records_loaded}")
    return records_loaded
