
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    timestamp_utc as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."int_hourly_weather_flagged"
where timestamp_utc is not null
group by timestamp_utc
having count(*) > 1



  
  
      
    ) dbt_internal_test