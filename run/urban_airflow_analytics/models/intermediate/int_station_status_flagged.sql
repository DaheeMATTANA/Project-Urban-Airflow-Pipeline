
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_flagged"
  
    as (
      

-- noqa: disable=RF02

WITH

station_status AS (
    SELECT
        station_id
        , last_reported_utc
        , last_reported_cet
        , is_installed
        , is_renting
        , is_returning
        , num_bikes_available
        , num_docks_available
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."stg_gbfs_station_status"
    
)

, station_capacity_info AS (
    SELECT
        station_id
        , capacity AS station_capacity
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."stg_gbfs_station_information"
)

, create_flags AS (
    SELECT
        station_status.*
        , station_capacity
        , COALESCE(num_bikes_available = station_capacity, FALSE) AS is_full
        , COALESCE(num_docks_available = station_capacity, FALSE) AS is_empty
        , station_capacity - (num_bikes_available + num_docks_available)
            AS num_bikes_in_maintenance
    FROM
        station_status
    LEFT JOIN
        station_capacity_info
        ON station_status.station_id = station_capacity_info.station_id
)

SELECT
    station_id
    , last_reported_utc
    , last_reported_cet
    , is_installed
    , is_renting
    , is_returning
    , num_bikes_available
    , num_docks_available
    , station_capacity
    , is_full
    , is_empty
    , num_bikes_in_maintenance
FROM
    create_flags
    );
  
  
  