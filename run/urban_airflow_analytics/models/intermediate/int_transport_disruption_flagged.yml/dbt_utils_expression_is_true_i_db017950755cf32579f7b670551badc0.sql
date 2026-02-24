
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."int_transport_disruption_flagged"

where not(started_at_cet <= ended_at_cet)


  
  
      
    ) dbt_internal_test