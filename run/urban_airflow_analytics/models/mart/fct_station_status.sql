
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."fct_station_status__dbt_tmp"
  
    as (
      WITH

active_station_status AS (
    SELECT *
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_aggregated"
    WHERE 1 = 1
    -- only the operational stations
    AND is_installed = TRUE
    AND is_renting = TRUE
    -- filter by the date of pipeline stabilisation
    AND last_reported_utc >= '2025-09-29'
    -- exclude incomplete reporting stations
    AND station_capacity IS NOT NULL
)

SELECT
    station_id
    , last_reported_cet
    , avg_num_bikes_available
    , avg_num_docks_available
    , pct_time_full
    , pct_time_empty
    , avg_num_bikes_in_maintenance
FROM
    active_station_status
    );
  
  