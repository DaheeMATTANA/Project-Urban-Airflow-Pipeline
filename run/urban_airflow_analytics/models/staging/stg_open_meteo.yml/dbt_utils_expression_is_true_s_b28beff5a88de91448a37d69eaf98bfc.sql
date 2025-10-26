
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."stg_open_meteo"

where not(timestamp_utc < timestamp_cet)


  
  
      
    ) dbt_internal_test