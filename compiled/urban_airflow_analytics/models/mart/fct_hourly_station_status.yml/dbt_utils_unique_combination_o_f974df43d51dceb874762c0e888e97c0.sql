





with validation_errors as (

    select
        station_id, last_reported_hourly_cet
    from "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_station_status"
    group by station_id, last_reported_hourly_cet
    having count(*) > 1

)

select *
from validation_errors


