
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        station_id, last_reported_cet
    from "warehouse_prod"."main_urban_airflow_analytics"."fct_station_status"
    group by station_id, last_reported_cet
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test