
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select station_id
from "dev_db"."raw"."raw_gbfs_station_status"
where station_id is null



  
  
      
    ) dbt_internal_test