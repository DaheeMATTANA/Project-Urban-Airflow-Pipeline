
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_planned
from "warehouse_prod"."main_urban_airflow_analytics"."fct_transport_disruption"
where is_planned is null



  
  
      
    ) dbt_internal_test