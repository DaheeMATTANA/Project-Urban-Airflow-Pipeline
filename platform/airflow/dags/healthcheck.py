from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(
    'healthcheck'
    , start_date = datetime(2025, 1, 1)
    , schedule = None
    , catchup = False
) : EmptyOperator(task_id = 'up')