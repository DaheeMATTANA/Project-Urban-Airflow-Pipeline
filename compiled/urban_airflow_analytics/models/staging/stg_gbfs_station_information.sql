WITH

source_gbfs_station_status AS (
    SELECT *
    FROM "dev_db"."raw"."raw_gbfs_station_information"
)

, renamed AS (
    SELECT
        station_id
        , stationcode AS station_code
        , name AS station_name
        , lat AS latitude
        , lon AS longitude
        , capacity
        , station_opening_hours
        , rental_methods
        , ingestion_date AS part_date_utc
        , ingestion_hour AS part_hour_utc
        , created_at AS loaded_at_utc
    FROM source_gbfs_station_status
)

SELECT
    station_id
    , station_code
    , station_name
    , latitude
    , longitude
    , capacity
    , station_opening_hours
    , rental_methods
    , part_date_utc
    , part_hour_utc
    , loaded_at_utc
FROM renamed