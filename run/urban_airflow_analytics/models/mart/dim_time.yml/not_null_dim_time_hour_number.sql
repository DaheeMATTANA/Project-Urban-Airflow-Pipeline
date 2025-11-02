
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour_number
from "warehouse_prod"."main_urban_airflow_analytics"."dim_time"
where hour_number is null



  
  
      
    ) dbt_internal_test