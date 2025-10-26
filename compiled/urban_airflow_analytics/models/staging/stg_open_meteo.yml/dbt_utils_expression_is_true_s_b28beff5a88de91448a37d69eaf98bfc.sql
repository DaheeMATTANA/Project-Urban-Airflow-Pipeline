



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."stg_open_meteo"

where not(timestamp_utc < timestamp_cet)

