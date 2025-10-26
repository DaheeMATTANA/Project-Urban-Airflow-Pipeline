
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."int_station_info_flagged__dbt_tmp"
  
    as (
      WITH

station_information AS (
    SELECT
        station_id
        , station_code
        , station_name
        , latitude
        , longitude
        , capacity
        , station_opening_hours
        , loaded_at_utc
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."stg_gbfs_station_information"
)

, add_valid_period AS (
    SELECT
        station_id
        , station_code
        , station_name
        , latitude
        , longitude
        , capacity
        , station_opening_hours
        , loaded_at_utc AS valid_from
        , LEAD(loaded_at_utc) OVER (
            PARTITION BY station_id
            ORDER BY loaded_at_utc
        ) AS valid_to
    FROM
        station_information
)

, add_flags AS (
    SELECT
        *
        , (valid_to IS NULL)
            AS is_current
        , COALESCE(station_opening_hours = 'DELETED', FALSE) AS is_deleted
    FROM
        add_valid_period
)

SELECT
    station_id
    , station_code
    , station_name
    , latitude
    , longitude
    , capacity
    , station_opening_hours
    , valid_from
    , valid_to
    , is_current
    , is_deleted
FROM
    add_flags
    );
  
  