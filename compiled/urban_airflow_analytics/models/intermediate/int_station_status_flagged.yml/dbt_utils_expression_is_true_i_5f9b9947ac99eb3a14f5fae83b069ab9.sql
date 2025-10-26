



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_flagged"

where not(num_bikes_available <= station_capacity)

