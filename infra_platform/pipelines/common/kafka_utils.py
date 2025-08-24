import json
import os

from kafka import KafkaProducer


def get_producer():
    """
    Create a Kafka/Redpanda producer with JSON serialisation.
    """
    broker = os.getenv("KAFKA_BROKER", "redpanda:19092")
    print(f"[DEBUG] Connecting to Kafka broker at {broker}")
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
    )
