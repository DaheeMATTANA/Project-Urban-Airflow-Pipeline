



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."stg_gbfs_station_status"

where not(last_reported_utc < last_reported_cet)

