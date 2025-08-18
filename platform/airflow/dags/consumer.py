import json
from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        "gbfs_station_status",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",     # start from beginning if no offset stored
        enable_auto_commit=True,
        group_id="test-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print("Listening for messages on 'gbfs_station_status'...")

    for msg in consumer:
        print(f"Received: {msg.value}")


if __name__ == "__main__":
    main()
