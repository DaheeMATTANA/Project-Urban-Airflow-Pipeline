import logging
import os
from datetime import UTC, datetime

from pipelines.common.duckdb_utils import get_duckdb_connection
from pipelines.common.load_state_utils import (
    get_last_loaded_at,
    update_last_loaded_at,
)


class BaseLoader:
    """
    Base class for all the loaders (Parquet).
    Provides:
        - DuckDB + MinIO setup
        - Schema-based table creation
        - Partition loading from S3 parquet
        - Incremental loading with checkpoints
        - Optional full refresh
    """

    def __init__(self, config):
        self.table_name = config["table_name"]
        self.bucket = config["bucket"]
        self.prefix = config["prefix"]
        self.schema = config["schema"]
        self.logger = logging.getLogger(f"loader.{self.table_name}")

    def setup_duckdb_s3(self, conn):
        """
        Common S3 configuration.
        """
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        conn.execute(f"""
            SET s3_endpoint="{endpoint}";
            SET s3_access_key_id="{access_key}";
            SET s3_secret_access_key="{secret_key}";
            SET s3_use_ssl=false;
            SET s3_url_style="path";
        """)

    def create_table(self, conn):
        """
        Create common table.
        """
        schema_sql = ",".join(
            [f"{col} {dtype}" for col, dtype in self.schema.items()]
        )

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {schema_sql},
                ingestion_date VARCHAR,
                ingestion_hour INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        self.logger.info(f"Table {self.table_name} ready")

    def build_s3_path(self, date_str, hour):
        """
        Default S3 path builder: s3://bucket/prefix/yyyy=YYYY/mm=MM/dd=DD/hh=HH/*.parquet
        Can be overridden by subclasses.
        """
        year, month, day = date_str.split("-")
        return f"s3a://{self.bucket}/{self.prefix}/yyyy={year}/mm={month}/dd={day}/hh={hour:02d}/*.parquet"

    def load_data(self, s3_path, date_str, hour, full_refresh=False):
        conn = get_duckdb_connection()
        try:
            self.create_table(conn)
            self.setup_duckdb_s3(conn)

            # Handle full refresh
            if full_refresh:
                self.logger.info(
                    f"[INFO] Full refresh enabled, wiping partition {date_str} hour {hour}"
                )
                conn.execute(
                    f"DELETE FROM {self.table_name} WHERE ingestion_date = '{date_str}' AND ingestion_hour = {hour}"
                )
                last_loaded_at = None
            else:
                last_loaded_at = get_last_loaded_at(conn, self.table_name)

            # Build explicit SELECT list in the same order as schema
            parquet_cols = ", ".join(
                [f"{col} AS {col}" for col in self.schema.keys()]
            )

            ts_col = "time" if "time" in self.schema else "timestamp"

            insert_sql = f"""
                INSERT INTO {self.table_name} ({", ".join(self.schema.keys())}, ingestion_date, ingestion_hour, created_at)
                SELECT 
                    {parquet_cols},
                    '{date_str}' as ingestion_date,
                    {hour} as ingestion_hour,
                    CURRENT_TIMESTAMP as created_at
                FROM read_parquet('{s3_path}')
            """

            if last_loaded_at:
                last_loaded_at = last_loaded_at.replace(tzinfo=None)
                insert_sql += f"WHERE {ts_col} > TIMESTAMP '{last_loaded_at}'"

            self.logger.info(
                f"[DEBUG] Loading from {s3_path}, last_loaded_at={last_loaded_at}, full_refresh={full_refresh}"
            )

            conn.execute(insert_sql)

            count = conn.execute(f"""
                SELECT COUNT(*) 
                FROM {self.table_name}
                WHERE ingestion_date = '{date_str}' AND ingestion_hour = {hour}
            """).fetchone()[0]

            if last_loaded_at:
                self.logger.info(
                    f"[DEBUG] Partition {date_str}/{hour}: {count} rows after incremental filter > {last_loaded_at}"
                )
            else:
                self.logger.info(
                    f"[DEBUG] Partition {date_str}/{hour}: {count} rows inserted (no filter)"
                )

            # Update checkpoint
            if count > 0:
                max_ts = conn.execute(
                    f"SELECT MAX({ts_col}) FROM {self.table_name}"
                ).fetchone()[0]
                if max_ts:
                    now = datetime.now(UTC).replace(tzinfo=None)
                    loaded_ts = min(max_ts, now)
                    update_last_loaded_at(conn, self.table_name, loaded_ts)
                    self.logger.info(
                        f"[INFO] Updated checkpoint for {self.table_name}: {loaded_ts}"
                    )

            conn.commit()
            self.logger.info(f"Loaded {count} rows from {s3_path}")
            return count
        finally:
            conn.close()

    def load_partition(self, date_str=None, hour=None, full_refresh=False):
        """
        High-level loader: DAGs should call this.
        Builds the correct s3_path internally.
        """
        if not date_str or hour is None:
            raise ValueError(
                "date_str and hour must be provided (use {{ ds }} and {{ logical_date.hour }} in DAG)"
            )

        s3_path = self.build_s3_path(date_str, hour)
        return self.load_data(s3_path, date_str, hour)

    def load_forecast_partition(self, date_str=None, full_refresh=False):
        """
        Special loader for forecast datasets (daily partitions, no hour) such as Open Meteo.
        Reuses the same incremental + checkpoint logic as load_partition.
        """
        if not date_str:
            raise ValueError("date_str must be provided for forecast loader")

        year, month, day = date_str.split("-")
        s3_path = f"s3a://{self.bucket}/{self.prefix}/yyyy={year}/mm={month}/dd={day}/*.parquet"

        return self.load_data(s3_path, date_str, 0, full_refresh=full_refresh)
