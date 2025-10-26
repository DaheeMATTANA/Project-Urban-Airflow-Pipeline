
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select timestamp_cet
from "warehouse_prod"."main_urban_airflow_analytics"."stg_open_meteo"
where timestamp_cet is null



  
  
      
    ) dbt_internal_test