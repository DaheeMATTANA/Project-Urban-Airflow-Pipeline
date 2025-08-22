import json
import os

from kafka import KafkaProducer


def get_producer():
    """
    Create a Kafka/Redpanda producer with JSON serialisation.
    """
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    print(f"[DEBUG] Connecting to Kafka broker at {broker}")
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
