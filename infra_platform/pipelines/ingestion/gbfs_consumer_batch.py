import json
import time

from kafka import KafkaConsumer
from pipelines.common.minio_utils import persist_message_to_minio

TOPIC = "gbfs_station_status"
BUCKET = "bronze"
PREFIX = "gbfs"


def consume_batch(max_messages=100, timeout_s=30):
    """
    Consume up to max_message or until timeout_s, then exit.
    Suitable for Airflow batch jobs.
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["redpanda:19092"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="gbfs-consumer-group",
    )

    start_time = time.time()
    count = 0

    print(f"Starting batch consume from {TOPIC}...")

    while count < max_messages and (time.time() - start_time) < timeout_s:
        # Poll returns a dict {TopicPartition: [messages]}
        records = consumer.poll(timeout_ms=1000)
        for _, msgs in records.items():
            for msg in msgs:
                message = msg.value
                persist_message_to_minio(BUCKET, PREFIX, message)
                count += 1
                if count >= max_messages:
                    break

    consumer.close()
    print(f"Batch consume complete: {count} messages persisted to MinIO")
