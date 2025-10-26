
    
    



select timestamp_utc
from "warehouse_prod"."main_urban_airflow_analytics"."stg_open_meteo_forecast"
where timestamp_utc is null


