import json, time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

def produce():
    p = KafkaProducer(bootstrap_servers='localhost:9092',
                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    p.send('gbfs_station_status', {'ts': datetime.utcnow().isoformat(), 'value': 1})
    p.flush()

with DAG('stream_dummy', start_date=datetime(2025,1,1), schedule_interval='* * * * *', catchup=False):
    PythonOperator(task_id='produce', python_callable=produce)