
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select holiday_date
from "warehouse_prod"."main_urban_airflow_analytics"."int_french_holidays"
where holiday_date is null



  
  
      
    ) dbt_internal_test