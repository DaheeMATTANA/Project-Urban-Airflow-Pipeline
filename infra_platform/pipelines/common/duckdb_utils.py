import os
from pathlib import Path

import duckdb


def get_duckdb_connection():
    """
    Get DuckDB connection to local database
    """
    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/raw.duckdb")

    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(db_path)
    return conn


def create_staging_table():
    """
    Create staging table if it doesn't exist
    """
    conn = get_duckdb_connection()

    create_sql = """
    CREATE TABLE IF NOT EXISTS staging__gbfs_station_status_raw(
        station_id VARCHAR
        , num_bikes_available INTEGER
        , num_docks_available INTEGER
        , is_installed BOOLEAN
        , is_returning BOOLEAN
        , is_renting BOOLEAN
        , last_reported TIMESTAMP
        , timestamp_cet_cest TIMESTAMP
        , ingestion_date DATE
        , ingestion_hour INTEGER
        , file_path VARCHAR
        , loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    conn.execute(create_sql)
    conn.commit()
    conn.close()
    print("Staging table created/verified")
