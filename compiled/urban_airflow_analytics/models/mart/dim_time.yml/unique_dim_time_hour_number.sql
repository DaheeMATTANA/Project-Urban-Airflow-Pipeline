
    
    

select
    hour_number as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."dim_time"
where hour_number is not null
group by hour_number
having count(*) > 1


