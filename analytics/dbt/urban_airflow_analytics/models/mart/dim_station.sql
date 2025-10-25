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
    FROM
        {{ ref('int_station_info_flagged') }}
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
FROM
    station_info_with_flag
