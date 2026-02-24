
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."int_station_info_flagged"

where not(valid_from <= valid_to)


  
  
      
    ) dbt_internal_test