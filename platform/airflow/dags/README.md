# Airflow Streaming DAGs

This folder contains Airflow DAGs for streaming pipelines using **Redpanda** (Kafka-compatible broker).

---

## DAGs

### `stream_dummy`
A minimal producer DAG that proves the Kafka pipeline end-to-end.

- **Schedule:** runs every minute (`* * * * *`)
- **Task:** produces one JSON message to topic `gbfs_station_status`
- **Message format:**
  ```json
  {
    "ts": "2025-08-19T08:12:00.123Z",
    "value": 1
  }

## Verification (DoD)

After starting the stack (`docker compose up`):

### 1. Check that the DAG is loaded
Open Airflow UI at [http://localhost:8081](http://localhost:8081) and confirm `stream_dummy` is visible.  
The `produce` task should run green every minute.

### 2. Check topic info (offsets should increase over time)
```bash
docker exec -it redpanda rpk topic describe gbfs_station_status