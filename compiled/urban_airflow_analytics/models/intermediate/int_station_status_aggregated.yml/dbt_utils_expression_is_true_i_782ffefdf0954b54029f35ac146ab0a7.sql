



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_aggregated"

where not(avg_num_docks_available <= station_capacity)

