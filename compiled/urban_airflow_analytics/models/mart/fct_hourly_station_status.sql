

-- noqa: disable=RF02

WITH

active_station_status AS (
    SELECT *
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."int_station_status_aggregated"
    WHERE
        1 = 1
        -- only the operational stations
        AND is_installed = TRUE
        AND is_renting = TRUE
        -- filter by the date of pipeline stabilisation
        AND last_reported_hourly_cet >= '2025-09-29'
        -- exclude incomplete reporting stations
        AND station_capacity IS NOT NULL
        
            AND
            last_reported_hourly_cet
            >= (
                SELECT MAX(last_reported_hourly_cet) - INTERVAL 2 HOUR
                FROM "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_station_status"
            )
        
)

, extract_date_hour AS (
    SELECT
        station_id
        , last_reported_hourly_cet
        , avg_num_bikes_available
        , avg_num_docks_available
        , pct_time_full
        , pct_time_empty
        , avg_num_bikes_in_maintenance
        , CAST(last_reported_hourly_cet AS DATE)
            AS last_reported_date_cet
        , HOUR(last_reported_hourly_cet)
            AS last_reported_hour_cet
    FROM
        active_station_status
)

SELECT
    station_id
    , last_reported_hourly_cet
    , last_reported_date_cet
    , last_reported_hour_cet
    , avg_num_bikes_available
    , avg_num_docks_available
    , pct_time_full
    , pct_time_empty
    , avg_num_bikes_in_maintenance
FROM
    extract_date_hour