# Platform

This folder contains the **data engineering platform layer** of Urban Airflow Pipeline.

It includes all components related to infrastructure, orchestration, and ingestion.

---

## 📂 Structure
- **airflow/** → DAGs, plugins, and Docker config to run Airflow locally.
- **pipelines/** → Python code for ingestion & streaming (batch APIs, producers/consumers).
- **minio/** → Local object storage (S3-compatible) replacing GCS buckets.
- **redpanda/** → Local Kafka-compatible broker for streaming.

---

## 🚀 Goals
- Provide a **local-first stack** that mirrors cloud-native architecture.
- Enable **streaming ingestion** (Redpanda) and **batch pipelines** (Airflow).
- Store data in **bronze/silver/gold/logs** buckets via MinIO.

---

## Airflow DAGs

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