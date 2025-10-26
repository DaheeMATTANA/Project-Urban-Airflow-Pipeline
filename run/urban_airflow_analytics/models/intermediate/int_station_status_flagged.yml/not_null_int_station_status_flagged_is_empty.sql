
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_empty
from "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_flagged"
where is_empty is null



  
  
      
    ) dbt_internal_test