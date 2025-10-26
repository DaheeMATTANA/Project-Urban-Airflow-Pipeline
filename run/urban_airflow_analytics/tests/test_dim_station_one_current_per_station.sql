
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT
    station_id
FROM 
    "warehouse_prod"."main_urban_airflow_analytics"."dim_station"
GROUP BY
    station_id
HAVING 1 = 1
    AND COUNT(DISTINCT is_current) > 1
  
  
      
    ) dbt_internal_test