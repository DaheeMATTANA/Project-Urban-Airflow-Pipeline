
    
    

select
    timestamp_utc as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."int_hourly_weather_flagged"
where timestamp_utc is not null
group by timestamp_utc
having count(*) > 1


