
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    date_cet as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."fct_daily_transport_disruption"
where date_cet is not null
group by date_cet
having count(*) > 1



  
  
      
    ) dbt_internal_test