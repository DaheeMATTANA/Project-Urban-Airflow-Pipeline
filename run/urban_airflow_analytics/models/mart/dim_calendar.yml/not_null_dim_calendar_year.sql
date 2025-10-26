
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "warehouse_prod"."main_urban_airflow_analytics"."dim_calendar"
where year is null



  
  
      
    ) dbt_internal_test