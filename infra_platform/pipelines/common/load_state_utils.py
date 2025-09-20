def get_last_loaded_at(conn, table_name: str):
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS meta;
        CREATE TABLE IF NOT EXISTS meta.load_state (
            table_name VARCHAR PRIMARY KEY
            , last_loaded_at TIMESTAMP
        );
    """)
    result = conn.execute(
        "SELECT last_loaded_at FROM meta.load_state WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return result[0] if result else None


def update_last_loaded_at(conn, table_name: str, last_ts):
    conn.execute(
        """
        INSERT INTO meta.load_state (table_name, last_loaded_at)
        VALUES (?, ?)
        ON CONFLICT (table_name) DO UPDATE SET last_loaded_at = excluded.last_loaded_at
    """,
        [table_name, last_ts],
    )
