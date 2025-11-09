
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        precipitation_probability_category as value_field,
        count(*) as n_records

    from "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_weather"
    group by precipitation_probability_category

)

select *
from all_values
where value_field not in (
    'low','medium','high'
)



  
  
      
    ) dbt_internal_test