import datetime

import pytz
from pipelines.common.http_utils import fetch_json_with_retry
from pipelines.common.kafka_utils import get_producer

URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
TOPIC = "gbfs_station_status"
CITY = "Paris"
paris_tz = pytz.timezone("Europe/Paris")


def produce():
    """
    Fetch GBFS station_status feed and publish to Redpanda/Kafka.
    """
    # Fetch data from API with retry/backoff
    data = fetch_json_with_retry(URL)

    # Enrich with metadata
    message = {
        "timestamp_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "timestamp_cet_cest": datetime.datetime.now(paris_tz).isoformat(),
        "city": "Paris",
        "payload": data,
    }

    # Create Kafka producer and send message
    producer = get_producer()
    producer.send(TOPIC, value=message)
    producer.flush()

    print(
        f"[{datetime.datetime.now(paris_tz).isoformat()}] Message sent to topic '{TOPIC}'"
    )


if __name__ == "__main__":
    produce()
