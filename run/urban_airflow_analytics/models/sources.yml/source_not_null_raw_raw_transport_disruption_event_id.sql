
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_id
from "dev_db"."raw"."raw_transport_disruption"
where event_id is null



  
  
      
    ) dbt_internal_test