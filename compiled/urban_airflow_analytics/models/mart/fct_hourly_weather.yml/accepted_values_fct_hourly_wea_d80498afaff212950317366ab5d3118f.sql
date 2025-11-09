
    
    

with all_values as (

    select
        temperature_category as value_field,
        count(*) as n_records

    from "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_weather"
    group by temperature_category

)

select *
from all_values
where value_field not in (
    'cold','mild','warm','hot'
)


