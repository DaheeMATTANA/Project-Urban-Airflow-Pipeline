
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."dim_station__dbt_tmp"
  
    as (
      WITH

station_info_with_flag AS (
    SELECT
        station_id
        , station_code
        , station_name
        , latitude
        , longitude
        , capacity
        , is_current
        , is_deleted
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."int_station_info_flagged"
    WHERE
        1 = 1
        AND station_id IS NOT NULL
)

SELECT
    station_id
    , station_code
    , station_name
    , latitude
    , longitude
    , capacity
    , is_current
    , is_deleted
FROM
    station_info_with_flag
    );
  
  