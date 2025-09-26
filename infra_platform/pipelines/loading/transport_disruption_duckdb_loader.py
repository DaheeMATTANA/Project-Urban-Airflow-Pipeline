import os
from datetime import UTC, datetime

from pipelines.common.duckdb_utils import get_duckdb_connection
from pipelines.loading.json_loader import JsonLoader

TRANSPORT_DISRUPTION_SCHEMA = {
    "event_id": "VARCHAR",
    "start_ts": "TIMESTAMP",
    "end_ts": "TIMESTAMP",
    "cause": "VARCHAR",
    "severity": "VARCHAR",
    "title": "VARCHAR",
    "message": "VARCHAR",
}


def _parse_ts(val: str):
    """
    Normalise timestamp strings into Python datetime.
    Handles both compact (YYYYMMDDTHHMMSS) and ISO formats.
    """
    if not val:
        return None
    try:
        if "T" in val and "-" not in val:
            # Compact style : 20251231T235900
            return datetime.strptime(val, "%Y%m%dT%H%M%S")
        # ISO 8601 style: 2025-12-31T23:59:00
        return datetime.fromisoformat(val)
    except Exception:
        return None


class DisruptionLoader(JsonLoader):
    def __init__(self):
        super().__init__(
            {
                "table_name": "raw.raw_transport_disruption",
                "bucket": os.getenv("MINIO_BUCKET", "bronze"),
                "prefix": "idfm_disruption",
                "schema": TRANSPORT_DISRUPTION_SCHEMA,
            }
        )

    def load_partition(self, date_str=None, hour=None, full_refresh=False):
        if not date_str or hour is None:
            raise ValueError("date_str and hour must be provided")

        conn = get_duckdb_connection()
        self.create_table(conn)

        # Get already ingested IDs
        existing_ids = set(
            conn.execute(f"""
            SELECT DISTINCT event_id
            FROM {self.table_name}
            """).fetchdf()["event_id"]
        )

        # Load MinIO JSONs
        import json

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
            conn.close()
            return 0

        df = pd.DataFrame(rows)

        # Drop duplicates inside the same batch
        before = len(df)
        df = df.drop_duplicates(subset=["event_id"])
        after1 = len(df)

        # Drop rows already in DB
        if existing_ids:
            df = df[~df["event_id"].isin(existing_ids)]
        after2 = len(df)

        self.logger.info(
            f"[DEBUG] Dedup inside batch removed {before - after1}, "
            f"DB dedup removed {after1 - after2}, "
            f"kept {after2} rows for {date_str} hour {hour}"
        )

        if df.empty:
            conn.close()
            return 0

        conn.register("tmp_df", df)
        cols = list(df.columns)
        conn.execute(f"""
            INSERT INTO {self.table_name} ({",".join(cols)})
            SELECT {",".join(cols)} FROM tmp_df
        """)

        count = len(df)
        conn.commit()
        conn.close()
        return count

    def flatten_records(self, data, date_str, hour):
        rows = []
        for d in data.get("disruptions", []):
            for ap in d.get("applicationPeriods", [{}]):
                rows.append(
                    {
                        "event_id": d.get("id"),
                        "start_ts": _parse_ts(ap.get("begin")),
                        "end_ts": _parse_ts(ap.get("end")),
                        "cause": d.get("cause"),
                        "severity": d.get("severity"),
                        "title": d.get("title"),
                        "message": d.get("message"),
                        "ingestion_date": date_str,
                        "ingestion_hour": hour,
                        "created_at": datetime.now(UTC).replace(tzinfo=None),
                    }
                )
        return rows
