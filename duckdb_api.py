import duckdb
from fastapi import FastAPI, Query

DB = "/home/dahee/Project-Urban-Airflow-Pipeline/infra_platform/duckdb_data/warehouse_prod.duckdb"
app = FastAPI()


@app.get("/query")
def query(sql: str = Query(...)):
    with duckdb.connect(DB, read_only=True) as con:
        df = con.execute(sql).fetch_df()
    return df.to_dict(orient="records")
