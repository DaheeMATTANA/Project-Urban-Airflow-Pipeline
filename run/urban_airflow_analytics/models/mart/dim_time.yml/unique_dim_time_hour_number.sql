
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    hour_number as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."dim_time"
where hour_number is not null
group by hour_number
having count(*) > 1



  
  
      
    ) dbt_internal_test