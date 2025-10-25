WITH

station_information AS (
    SELECT
        station_id
        , station_code
        , station_name
        , latitude
        , longitude
        , capacity
        , loaded_at_utc
    FROM
        {{ ref('stg_gbfs_station_information') }}
)

, valid_flag AS (
    SELECT
        station_id
        , station_code
        , station_name
        , latitude
        , longitude
        , capacity
        , loaded_at_utc AS valid_from
        , LEAD(loaded_at_utc) OVER (
            PARTITION BY station_id
            ORDER BY loaded_at_utc
        ) AS valid_to
        , valid_to IS NULL
            AS is_current
    FROM
        station_information
)

SELECT
    station_id
    , station_code
    , station_name
    , latitude
    , longitude
    , capacity
    , valid_from
    , valid_to
    , is_current
FROM
    valid_flag
