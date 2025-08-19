# Airflow DAGs

This folder contains Airflow DAGs for streaming pipelines using **Redpanda** (Kafka-compatible broker).

### Healthcheck DAG

Un DAG trivial (`healthcheck`) est fourni pour vérifier que le scheduler fonctionne.

**UI :**
1. Dans la liste des DAGs, activer `healthcheck`
2. Cliquer sur le bouton ▶️ *Trigger DAG*
3. Vérifier que le run obtient un ✅ vert dans les 30 secondes

**CLI (dans le container scheduler) :**
```bash
# Lancer le DAG
docker exec -it airflow-scheduler \
  airflow dags trigger healthcheck

# Vérifier l’état du dernier run
docker exec -it airflow-scheduler \
  airflow dags state healthcheck $(date +%Y-%m-%d)T00:00:00+00:00
```

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
Open Airflow UI and confirm `stream_dummy` is visible.  
The `produce` task should run green every minute.

### 2. Check topic info (offsets should increase over time)
```bash
docker exec -it redpanda rpk topic describe gbfs_station_status
