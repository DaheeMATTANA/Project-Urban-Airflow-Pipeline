



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."int_hourly_weather_flagged"

where not(timestamp_utc < timestamp_cet)

