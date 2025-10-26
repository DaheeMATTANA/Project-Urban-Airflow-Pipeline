
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_aggregated"
  
    as (
      

WITH

station_status AS (
    SELECT *
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_flagged"
    
)

, aggregated_by_minute AS (
    SELECT DISTINCT ON (station_id, last_reported_utc)
        station_id
        , last_reported_utc
        , is_installed
        , is_renting
        , is_returning
        , station_capacity
        , AVG(num_bikes_available) OVER (
            PARTITION BY station_id
            ORDER BY last_reported_utc
        ) AS avg_num_bikes_available
        , AVG(num_docks_available) OVER (
            PARTITION BY station_id
            ORDER BY last_reported_utc
        ) AS avg_num_docks_available
        , AVG(CASE WHEN is_full = TRUE THEN 1 ELSE 0 END) OVER (
            PARTITION BY station_id
            ORDER BY last_reported_utc
        ) AS pct_time_full
        , AVG(CASE WHEN is_empty = TRUE THEN 1 ELSE 0 END) OVER (
            PARTITION BY station_id
            ORDER BY last_reported_utc
        ) AS pct_time_empty
        , AVG(num_bikes_in_maintenance) OVER (
            PARTITION BY station_id
            ORDER BY last_reported_utc
        ) AS avg_num_bikes_in_maintenance
    FROM
        station_status
)

, added_timezone_cet AS (
    SELECT
        station_id
        , last_reported_utc
        , is_installed
        , is_renting
        , is_returning
        , station_capacity
        , avg_num_bikes_available
        , avg_num_docks_available
        , pct_time_full
        , pct_time_empty
        , avg_num_bikes_in_maintenance
        , last_reported_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Paris'
            AS last_reported_cet
    FROM
        aggregated_by_minute
)

SELECT
    station_id
    , last_reported_utc
    , last_reported_cet
    , is_installed
    , is_renting
    , is_returning
    , station_capacity
    , avg_num_bikes_available
    , avg_num_docks_available
    , pct_time_full
    , pct_time_empty
    , avg_num_bikes_in_maintenance
FROM
    added_timezone_cet
    );
  
  
  