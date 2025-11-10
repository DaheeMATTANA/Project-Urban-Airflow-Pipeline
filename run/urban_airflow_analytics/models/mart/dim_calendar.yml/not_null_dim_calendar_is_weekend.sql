
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_weekend
from "warehouse_prod"."main_urban_airflow_analytics"."dim_calendar"
where is_weekend is null



  
  
      
    ) dbt_internal_test