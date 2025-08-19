import json, time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from kafka.errors import KafkaError

def produce():
    retries = 5
    backoff = 5  # seconds

    for attempt in range(1, retries+1):
        try:
            producer = KafkaProducer(
                bootstrap_servers='redpanda:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=10,
                acks='all'
            )

            message = {
                'ts': datetime.utcnow().isoformat(),
                'value': 1
            }

            future = producer.send('gbfs_station_status', value=message)
            record_metadata = future.get(timeout=10)

            print(
                f'Produced message to {record_metadata.topic}'
                f'[partition {record_metadata.partition}, offset {record_metadata.offset}]'
            )

            producer.fluch()
            producer.close()
            break # success, exit loop

        except KafkaError as e:
            print(f'Kafka error on attempt {attempt}/{retries}: {e}')
            if attempt < retries:
                time.sleep(backoff * attempt)
            else:
                raise e
            
# ---- Airflow DAG ----
with DAG(
    dag_id='stream_dummy',
    start_date=datetime(2025,1,1),
    schedule_interval='* * * * *', # every minute
    catchup=False,
    tags=['streaming', 'kafka', 'redpanda'],
) as dag:
    produce_task = PythonOperator(
        task_id='produce',
        python_callable=produce,
    )
