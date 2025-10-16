WITH

station_status AS (
    SELECT *
    FROM
        {{ ref('stg_gbfs_station_status') }}
)

, aggregated_by_minute AS (
    SELECT DISTINCT ON (station_id, last_reported_utc)
        station_id
        , last_reported_utc
        , is_installed
        , is_renting
        , is_returning
        , AVG(num_bikes_available) OVER (
            PARTITION BY station_id
            ORDER BY last_reported_utc
        ) AS avg_num_bikes_available
        , AVG(num_docks_available) OVER (
            PARTITION BY station_id
            ORDER BY last_reported_utc
        ) AS avg_num_docks_available
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
        , avg_num_bikes_available
        , avg_num_docks_available
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
    , avg_num_bikes_available
    , avg_num_docks_available
FROM
    added_timezone_cet
