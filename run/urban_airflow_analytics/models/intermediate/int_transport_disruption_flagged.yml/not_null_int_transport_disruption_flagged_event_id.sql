
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from "warehouse_prod"."main_urban_airflow_analytics"."int_transport_disruption_flagged"
where event_id is null



  
  
      
    ) dbt_internal_test