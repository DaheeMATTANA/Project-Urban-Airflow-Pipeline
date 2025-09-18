import logging
import os

from pipelines.common.duckdb_utils import get_duckdb_connection


class BaseLoader:
    """
    Base class for all the loaders.
    Provides:
        - DuckDB + MinIO setup
        - Schema-based table creation
        - Partition loading from S3 parquet
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

        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
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

    def load_data(self, s3_path, date_str, hour):
        conn = get_duckdb_connection()
        try:
            self.create_table(conn)
            self.setup_duckdb_s3(conn)

            # Build explicit SELECT list in the same order as schema
            parquet_cols = ", ".join(
                [f"{col} AS {col}" for col in self.schema.keys()]
            )

            insert_sql = f"""
                INSERT INTO {self.table_name} ({", ".join(self.schema.keys())}, ingestion_date, ingestion_hour, created_at)
                SELECT 
                    {parquet_cols},
                    '{date_str}' as ingestion_date,
                    {hour} as ingestion_hour,
                    CURRENT_TIMESTAMP as created_at
                FROM read_parquet('{s3_path}')
            """

            conn.execute(insert_sql)

            count = conn.execute(f"""
                SELECT COUNT(*) 
                FROM {self.table_name}
                WHERE ingestion_date = '{date_str}' AND ingestion_hour = {hour}
            """).fetchone()[0]
            conn.commit()

            self.logger.info(f"Loaded {count} rows from {s3_path}")
            return count
        finally:
            conn.close()

    def load_partition(self, date_str, hour):
        """
        High-level loader: DAGs should call this.
        Builds the correct s3_path internally.
        """
        s3_path = self.build_s3_path(date_str, hour)
        return self.load_data(s3_path, date_str, hour)
