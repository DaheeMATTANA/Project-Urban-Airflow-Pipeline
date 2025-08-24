import json

from kafka import KafkaConsumer
from pipelines.common.minio_utils import persist_message_to_minio

TOPIC = "gbfs_station_status"
BUCKET = "bronze"
PREFIX = "gbfs"


def consume():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["redpanda:9092"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="gbfs-consumer-group",
    )

    print(f"Listening to topic {TOPIC}...")

    for msg in consumer:
        message = msg.value
        persist_message_to_minio(BUCKET, PREFIX, message)
