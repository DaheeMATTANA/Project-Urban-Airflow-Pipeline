
    
    

with all_values as (

    select
        wind_category as value_field,
        count(*) as n_records

    from "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_weather"
    group by wind_category

)

select *
from all_values
where value_field not in (
    'calm','breezy','windy','storm'
)


