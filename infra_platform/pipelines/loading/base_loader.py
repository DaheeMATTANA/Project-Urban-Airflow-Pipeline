import logging
import os

from pipelines.common.duckdb_utils import get_duckdb_connection


class BaseLoader:
    """
    Base class for all the loaders.
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

    def load_data(self, s3_path, date_str, hour):
        """
        Base method for loading data.
        """
        conn = get_duckdb_connection()

        try:
            self.create_table(conn)
            self.setup_duckdb_s3(conn)

            base_columns = ", ".join(self.schema.keys())

            # Insert SQL
            conn.execute(f"""
                INSERT INTO {self.table_name} 
                SELECT 
                    {base_columns},
                    '{date_str}' as ingestion_date,
                    {hour} as ingestion_hour,
                    CURRENT_TIMESTAMP as created_at
                FROM read_parquet('{s3_path}')
            """)

            # Compute results
            count = conn.execute(
                "SELECT COUNT(*) FROM (SELECT 1) as dummy_count"
            ).fetchone()[0]
            conn.commit()

            self.logger.info(f"Loaded {count} rows from {s3_path}")
            return count

        finally:
            conn.close()
