from datetime import timedelta

from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "team-buldo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}
