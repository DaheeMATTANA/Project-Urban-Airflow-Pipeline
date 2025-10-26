
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_deleted
from "warehouse_prod"."main_urban_airflow_analytics"."int_station_info_flagged"
where is_deleted is null



  
  
      
    ) dbt_internal_test