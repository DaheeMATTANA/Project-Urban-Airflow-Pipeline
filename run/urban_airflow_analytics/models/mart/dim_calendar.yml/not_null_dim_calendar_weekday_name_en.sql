
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select weekday_name_en
from "warehouse_prod"."main_urban_airflow_analytics"."dim_calendar"
where weekday_name_en is null



  
  
      
    ) dbt_internal_test