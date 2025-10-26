
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_flagged"

where not(num_docks_available <= station_capacity)


  
  
      
    ) dbt_internal_test