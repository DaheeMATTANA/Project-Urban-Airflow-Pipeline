
    
    

select
    holiday_date as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."dim_french_holidays"
where holiday_date is not null
group by holiday_date
having count(*) > 1


