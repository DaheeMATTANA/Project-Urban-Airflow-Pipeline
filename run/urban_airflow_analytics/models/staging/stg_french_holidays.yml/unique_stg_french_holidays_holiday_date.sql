
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    holiday_date as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."stg_french_holidays"
where holiday_date is not null
group by holiday_date
having count(*) > 1



  
  
      
    ) dbt_internal_test