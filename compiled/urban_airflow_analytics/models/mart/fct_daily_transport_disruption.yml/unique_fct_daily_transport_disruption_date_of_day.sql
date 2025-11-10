
    
    

select
    date_of_day as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."fct_daily_transport_disruption"
where date_of_day is not null
group by date_of_day
having count(*) > 1


