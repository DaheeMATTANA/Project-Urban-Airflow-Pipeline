
    
    



select timestamp_utc
from "warehouse_prod"."main_urban_airflow_analytics"."int_hourly_weather_flagged"
where timestamp_utc is null


