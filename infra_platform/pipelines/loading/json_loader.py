import os
from datetime import UTC

from pipelines.common.duckdb_utils import get_duckdb_connection
from pipelines.common.load_state_utils import (
    get_last_loaded_at,
    update_last_loaded_at,
)
from pipelines.loading.base_loader import BaseLoader


class JsonLoader(BaseLoader):
    """
    Loader for JSON data stored in MinIO.
    Subclass of BaseLoader, but overrides file-reading logic
    to use DuckDB's read_json_auto instead of read_parquet.
    """

    def build_s3_path(self, date_str, hour):
        """
        Override default S3 path builder from JSON ingestion layout:
        s3a://bucket/prefix/date=YYYY-MM-DD/hour=HH/*.json
        """
        return f"s3a://{self.bucket}/{self.prefix}/date={date_str}/hour-{hour:02d}/*.json"

    def load_data(self, s3_path, date_str, hour, full_refresh=False):
        """
        Override load_data to read JSON instead of Parquet.
        Keeps checkpoint logic, table creation, and logging.
        """
        conn = get_duckdb_connection()
        conn.execute("INSTALL json;")
        conn.execute("LOAD json;")

        try:
            self.create_table(conn)
            self.setup_duckdb_s3(conn)

            self.logger.info(
                f"[DEBUG] Using DuckDB S3 config for endpoint={os.getenv('MINIO_ENDPOINT')}, bucket={self.bucket}"
            )
            res = conn.execute("SHOW ALL;").fetchall()
            self.logger.info(f"[DEBUG] Loaded extensions: {res}")

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

            ts_col = (
                "start_ts"
                if "start_ts" in self.schema
                else "time"
                if "time" in self.schema
                else "timestamp"
            )

            if hasattr(self, "flatten_records"):
                self.logger.info("[DEBUG] Using Python flattening mode")

                import json
                from datetime import datetime

                import pandas as pd
                from pipelines.common.minio_utils import get_minio_client

                minio = get_minio_client()
                objects = minio.list_objects(
                    self.bucket,
                    prefix=f"{self.prefix}/date={date_str}/hour-{hour:02d}/",
                    recursive=True,
                )

                rows = []
                for obj in objects:
                    if not obj.object_name.endswith(".json"):
                        continue
                    resp = minio.get_object(self.bucket, obj.object_name)
                    data = json.loads(resp.read().decode("utf-8"))
                    resp.close()
                    resp.release_conn()

                    rows.extend(self.flatten_records(data, date_str, hour))

                if not rows:
                    self.logger.info(
                        f"[DEBUG] No rows to insert for {date_str} hour {hour}"
                    )
                    return 0

                df = pd.DataFrame(rows)

                if "start_ts" in df.columns:
                    df["start_ts"] = pd.to_datetime(
                        df["start_ts"], errors="coerce"
                    )
                if last_loaded_at and "start_ts" in df.columns:
                    df = df[df["start_ts"] > last_loaded_at]
                if df.empty:
                    self.logger.info(
                        f"[DEBUG] Skipping insert, no new rows beyond {last_loaded_at}"
                    )
                    return 0

                conn.register("tmp_df", df)
                cols = list(df.columns)
                conn.execute(f"""
                    INSERT OR REPLACE INTO {self.table_name} ({",".join(cols)})
                    SELECT {",".join(cols)} FROM tmp_df
                """)

                count = len(df)

            else:
                if hasattr(self, "build_insert_sql"):
                    insert_sql = self.build_insert_sql(
                        s3_path, date_str, hour, last_loaded_at
                    )

                else:
                    json_cols = ", ".join(self.schema.keys())

                    insert_sql = f"""
                        INSERT INTO {self.table_name} ({",".join(self.schema.keys())}, ingestion_date, ingestion_hour, created_at)
                        SELECT
                            {json_cols}
                            , '{date_str}' AS ingestion_date
                            , {hour} AS ingestion_hour
                            , CURRENT_TIMESTAMP AS created_at
                        FROM read_json_auto('{s3_path}')
                    """

                    if last_loaded_at and not hasattr(
                        self, "build_insert_sql"
                    ):
                        last_loaded_at = last_loaded_at.replace(tzinfo=None)
                        insert_sql += (
                            f" WHERE {ts_col} > TIMESTAMP '{last_loaded_at}'"
                        )

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
