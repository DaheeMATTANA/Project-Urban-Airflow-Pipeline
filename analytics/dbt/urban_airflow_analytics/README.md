Welcome to Urban Airflow Analytics dbt Project!

### Access to dbt GUI

```bash
cd infra_platform/airflow/docker
docker compose exec -it airflow-webserver bash

cd /opt/airflow/src/analytics/dbt/urban_airflow_analytics
dbt docs generate
dbt docs serve --port 8082 --host 0.0.0.0
