
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_of_day
from "warehouse_prod"."main_urban_airflow_analytics"."dim_calendar"
where date_of_day is null



  
  
      
    ) dbt_internal_test