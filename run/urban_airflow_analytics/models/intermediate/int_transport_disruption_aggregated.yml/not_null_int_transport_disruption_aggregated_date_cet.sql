
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_cet
from "warehouse_prod"."main_urban_airflow_analytics"."int_transport_disruption_aggregated"
where date_cet is null



  
  
      
    ) dbt_internal_test