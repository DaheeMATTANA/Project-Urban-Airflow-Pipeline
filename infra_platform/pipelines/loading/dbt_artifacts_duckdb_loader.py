import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb


def load_dbt_test_results(env="dev"):
    """
    Load dbt test results (from run_results.json) into DuckDB table dbt_table_results.
    """

    warehouse = f"/opt/airflow/data/warehouse_{env}.duckdb"
    target = Path(
        "/opt/airflow/src/analytics/dbt/urban_airflow_analytics/target"
    )

    # Load artifiacts
    with open(target / "run_results.json") as f:
        run_results = json.load(f)["results"]

    # Connect to DuckDB
    Path(warehouse).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(warehouse)
    print(f"Connected to warehouse {warehouse}")

    conn.execute("CREATE SCHEMA IF NOT EXISTS meta")

    # Create table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.dbt_test_results (
            test_name TEXT
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
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    result["unique_id"],
                    result["status"],
                    result.get("execution_time", 0),
                    result.get("message", ""),
                    now,
                ],
            )

    conn.commit()
    conn.close()
    print("DBT test results loaded into DuckDB 'meta.dbt_test_results")
