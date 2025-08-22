import json

from kafka import KafkaProducer


def get_producer(bootstrap_servers: str = "redpanda:9092") -> KafkaProducer:
    """
    Create a Kafka/Redpanda producer with JSON serialisation.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
