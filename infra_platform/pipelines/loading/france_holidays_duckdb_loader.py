import json
import logging
import os
from datetime import UTC, datetime

from minio import Minio
from pipelines.common.duckdb_utils import get_duckdb_connection
from pipelines.loading.base_loader import BaseLoader

logger = logging.getLogger("loader.holidays")


class HolidaysLoader(BaseLoader):
    """
    Loader for French holidays JSON stored in MinIO.
    Inherits BaseLoader but overrides path logic & loading method.
    """

    def __init__(self, min_year: int):
        config = {
            "table_name": "raw.raw_french_holidays",
            "bucket": os.getenv("MINIO_BUCKET", "bronze"),
            "prefix": "holidays_fr",
            "schema": {
                "date": "DATE",
                "holiday_name": "VARCHAR",
                "is_national": "BOOLEAN",
            },
        }
        super().__init__(config)

        self.min_year = min_year

        # Override MinIO client
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )

    def load_json_files(self, full_refresh: bool = True):
        """
        Load all holiday JSON files from MinIO into DuckDB.
        Ignores checkpoints since dataset is small and static.
        """
        conn = get_duckdb_connection()
        try:
            self.create_table(conn)

            if full_refresh:
                conn.execute(f"DELETE FROM {self.table_name}")

            objects = self.minio_client.list_objects(
                self.bucket, prefix=self.prefix, recursive=True
            )

            total = 0
            for obj in objects:
                if not obj.object_name.endswith(".json"):
                    continue

                resp = self.minio_client.get_object(
                    self.bucket, obj.object_name
                )
                data = json.load(resp)

                now = datetime.now(UTC)
                ingestion_date = now.strftime("%Y-%m-%d")
                ingestion_hour = now.hour

                rows = [
                    (d, name, True, ingestion_date, ingestion_hour)
                    for d, name in data.items()
                    if int(d.split("-")[0]) >= self.min_year
                ]

                if not rows:
                    continue

                conn.executemany(
                    f"""
                    INSERT INTO {self.table_name} (date, holiday_name, is_national, ingestion_date, ingestion_hour)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    rows,
                )

                count = len(rows)
                total += count
                logger.info(
                    f"Inserted {count} holidays from {obj.object_name}"
                )

            conn.commit()
            logger.info(
                f"Total {total} holidays inserted into {self.table_name}"
            )
            return total

        except Exception as e:
            logger.error(f"Failed loading holidays: {e}")
            raise

        finally:
            conn.close()


if __name__ == "__main__":
    loader = HolidaysLoader(min_year=2025)
    loader.load_json_files()
