import hashlib
import json
import os
from datetime import UTC

from pipelines.common.duckdb_utils import get_duckdb_connection
from pipelines.common.load_state_utils import (
    get_last_loaded_at,
    update_last_loaded_at,
)
from pipelines.loading.base_loader import BaseLoader


def _compute_row_hash(row: dict, exclude_cols=None) -> str:
    """
    A helper to compute a deterministic md5 hash for change detection.
    """
    exclude_cols = exclude_cols or []
    filtered = {k: v for k, v in row.items() if k not in exclude_cols}
    payload = json.dumps(filtered, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


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

    def get_minio_prefix(self, date_str, hour):
        return f"{self.prefix}/date={date_str}/hour-{hour:02d}/"

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

            if "start_ts" in self.schema:
                ts_col = "start_ts"
            elif "time" in self.schema:
                ts_col = "time"
            elif "timestamp" in self.schema:
                ts_col = "timestamp"
            else:
                ts_col = None

            if hasattr(self, "flatten_records"):
                self.logger.info("[DEBUG] Using Python flattening mode")

                import json
                from datetime import datetime

                import pandas as pd
                from pipelines.common.minio_utils import get_minio_client

                minio = get_minio_client()
                objects = minio.list_objects(
                    self.bucket,
                    prefix=self.get_minio_prefix(date_str, hour),
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

                # Incremental load based on timestamp (generic)
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

                # Incremental load based on content hash for staion_information (optional)
                if "record_hash" in df.columns and "station_id" in df.columns:
                    existing = conn.execute(f"""
                        SELECT station_id, record_hash
                        FROM {self.table_name}
                    """).fetchdf()

                    existing["station_id"] = existing["station_id"].astype(str)
                    df["station_id"] = df["station_id"].astype(str)

                    # Detect stations that disappeared from the API
                    existing_ids = set(existing["station_id"])
                    new_ids = set(df["station_id"])

                    existing_map = dict(
                        zip(
                            existing["station_id"],
                            existing["record_hash"],
                            strict=False,
                        )
                    )
                    before = len(df)

                    # Keep only new or changed rows
                    df = df[
                        df.apply(
                            lambda r: existing_map.get(r["station_id"])
                            != r["record_hash"],
                            axis=1,
                        )
                    ]
                    after = len(df)
                    self.logger.info(
                        f"[DEBUG] Filtered {before - after} unchanged rows; keeping {after} new/changed"
                    )

                    deleted_ids = existing_ids - new_ids

                    if deleted_ids:
                        self.logger.info(
                            f"[INFO] Found {len(deleted_ids)} deleted stations: inserting 'DELETED' rows"
                        )
                        import pandas as pd

                        deleted_df = pd.DataFrame(
                            [
                                {
                                    "station_id": sid,
                                    "record_hash": "deleted",
                                    "station_opening_hours": "DELETED",
                                    "ingestion_date": date_str,
                                    "ingestion_hour": hour,
                                    "created_at": pd.Timestamp.utcnow(),
                                }
                                for sid in deleted_ids
                            ]
                        )

                        valid_cols = [
                            c
                            for c in deleted_df.columns
                            if c in self.schema.keys()
                            or c
                            in [
                                "ingestion_date",
                                "ingestion_hour",
                                "created_at",
                            ]
                        ]
                        deleted_df = deleted_df[valid_cols]

                        # Register and insert as new rows (no update)
                        conn.register("deleted_tmp", deleted_df)
                        cols = list(deleted_df.columns)
                        conn.execute(f"""
                            INSERT INTO {self.table_name} ({",".join(cols)})
                            SELECT {",".join(cols)} FROM deleted_tmp
                        """)

                    if df.empty:
                        self.logger.info(
                            "[INFO] No changed stations — nothing to load"
                        )
                        return 0

                conn.register("tmp_df", df)
                cols = list(df.columns)

                sql = f"""
                    INSERT OR REPLACE INTO {self.table_name} ({",".join(cols)})
                    SELECT {",".join(cols)} FROM tmp_df
                """
                self.logger.info(
                    f"[DEBUG] Executing INSERT OR REPLACE for {self.table_name}"
                )
                conn.execute(sql)

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

            if count > 0 and ts_col:
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
            else:
                self.logger.info(
                    "[INFO] Skipping checkpoint: no timestamp column"
                )

            conn.commit()
            self.logger.info(f"Loaded {count} rows from {s3_path}")
            return count
        finally:
            conn.close()
