
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        general_weather_condition as value_field,
        count(*) as n_records

    from "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_weather"
    group by general_weather_condition

)

select *
from all_values
where value_field not in (
    'clear','cloudy','rain','snow','fog'
)



  
  
      
    ) dbt_internal_test