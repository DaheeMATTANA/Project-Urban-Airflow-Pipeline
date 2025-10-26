
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    date_of_day as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."dim_calendar"
where date_of_day is not null
group by date_of_day
having count(*) > 1



  
  
      
    ) dbt_internal_test