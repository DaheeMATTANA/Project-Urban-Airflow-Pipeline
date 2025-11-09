
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        anormality_category as value_field,
        count(*) as n_records

    from "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_weather"
    group by anormality_category

)

select *
from all_values
where value_field not in (
    'none','anormal','very anormal'
)



  
  
      
    ) dbt_internal_test