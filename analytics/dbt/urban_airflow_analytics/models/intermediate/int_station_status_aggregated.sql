{{ 
    config(
        materialized = 'incremental'
        , unique_key = ['station_id', 'last_reported_hourly_utc']
) }}

-- noqa: disable=RF02, LT05

WITH

station_status AS (
    SELECT *
    FROM
        {{ ref('int_station_status_flagged') }}
    {% if is_incremental() %}
        WHERE
            last_reported_utc
            >= (SELECT MAX(last_reported_hourly_utc) - INTERVAL 2 HOUR FROM {{ this }})
    {% endif %}
)

, aggregated_by_hour AS (
    SELECT
        station_id
        , DATE_TRUNC('hour', last_reported_utc) AS last_reported_hourly_utc
        , MAX(is_installed) AS is_installed
        , MAX(is_renting) AS is_renting
        , MAX(is_returning) AS is_returning
        , MAX(station_capacity) AS station_capacity
        , AVG(num_bikes_available) AS avg_num_bikes_available
        , AVG(num_docks_available) AS avg_num_docks_available
        , AVG(CASE WHEN is_full = TRUE THEN 1 ELSE 0 END) AS pct_time_full
        , AVG(CASE WHEN is_empty = TRUE THEN 1 ELSE 0 END) AS pct_time_empty
        , AVG(num_bikes_in_maintenance) AS avg_num_bikes_in_maintenance
    FROM
        station_status
    GROUP BY
        station_id
        , DATE_TRUNC('hour', last_reported_utc)
)

, added_timezone_cet AS (
    SELECT
        station_id
        , last_reported_hourly_utc
        , is_installed
        , is_renting
        , is_returning
        , station_capacity
        , avg_num_bikes_available
        , avg_num_docks_available
        , pct_time_full
        , pct_time_empty
        , avg_num_bikes_in_maintenance
        , last_reported_hourly_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Paris'
            AS last_reported_hourly_cet
    FROM
        aggregated_by_hour
)

SELECT
    station_id
    , last_reported_hourly_utc
    , last_reported_hourly_cet
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
