from datetime import UTC, datetime

from pipelines.common.duckdb_utils import get_duckdb_connection
from pipelines.loading.base_loader import BaseLoader


class OpenAQLoader(BaseLoader):
    """
    Loader specific to OpenAQ
    """

    def __init__(self):
        config = {
            "table_name": "staging__openaq_raw",
            "bucket": "raw",
            "prefix": "openaq",
            "schema": {
                "timestamp": "TIMESTAMP",
                "value": "DOUBLE",
                "unit": "VARCHAR",
                "sensor_id": "VARCHAR",
                "parameter": "VARCHAR",
                "location": "VARCHAR",
            },
        }
        super().__init__(config)

    def build_s3_path(self, date_str, hour):
        """
        Build S3 path OpenAQ
        """
        year, month, day = date_str.split("-")
        return f"s3://{self.bucket}/{self.prefix}/yyyy={year}/MM={month}/dd={day}/hh={hour:02d}/*.parquet"

    def load_current_hour(self):
        """
        Load currnet hour
        """
        now = datetime.now(UTC)
        date_str = now.strftime("%Y-%m-%d")
        hour = now.hour

        S3_path = self.build_s3_path(date_str, hour)
        return self.load_data(S3_path, date_str, hour)

    def load_specific_hour(self, date_str, hour):
        """
        Load a specific hour
        """
        s3_path = self.build_s3_path(date_str, hour)
        return self.load_data(s3_path, date_str, hour)

    def get_air_quality_summary(self):
        """
        Get air quality summary.
        """
        conn = get_duckdb_connection()
        try:
            results = conn.execute("""
                SELECT
                    parameter
                    , COUNT(*) as records
                    , AVG(value) as avg_value
                    , MAX(timestamp) as latest
                FROM staging__openaq_raw
                WHERE parameter IN('PM2.5', 'PM10', 'NO2')
                GROUP BY parameter
                ORDER BY parameter
            """).fetchall()

            self.logger.info("=== AIR QUALITY SUMMARY ===")
            for param, count, avg_val, _latest in results:
                self.logger.info(
                    f"{param}: {count} records, avg={avg_val:.2f}"
                )

            return results
        finally:
            conn.close()
