import json
from datetime import UTC, datetime
from pathlib import Path

from pipelines.common.duckdb_utils import get_duckdb_connection


def load_dbt_test_results(env="dev"):
    """
    Load dbt test results (from run_results.json) into DuckDB table dbt_table_results.
    """

    target = Path(
        "/opt/airflow/src/analytics/dbt/urban_airflow_analytics/target"
    )

    # Load artifiacts
    with open(target / "run_results.json") as f:
        run_results = json.load(f)["results"]

    # Connect to DuckDB
    conn = get_duckdb_connection()

    conn.execute("CREATE SCHEMA IF NOT EXISTS meta")

    # Create table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.dbt_test_results (
            environment TEXT
            , test_name TEXT
            , status TEXT
            , execution_time DOUBLE
            , message TEXT
            , loaded_at_utc TIMESTAMP
        );
    """)

    now = datetime.now(UTC)

    for result in run_results:
        if result["unique_id"].startswith("test."):
            conn.execute(
                """
                INSERT INTO meta.dbt_test_results
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    env,
                    result["unique_id"],
                    result["status"],
                    result.get("execution_time", 0),
                    result.get("message", ""),
                    now,
                ],
            )

    conn.commit()
    conn.close()
    print(f"DBT {env} test results loaded into DuckDB 'meta.dbt_test_results")
