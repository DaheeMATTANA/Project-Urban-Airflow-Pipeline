WITH

source_gbfs_station_status AS (
    SELECT *
    FROM {{ source('raw', 'raw_gbfs_station_status') }}
)

, renamed AS (
    SELECT
        station_id
        , num_bikes_available
        , num_docks_available
        , is_installed
        , is_renting
        , last_reported AS last_reported_utc
        , timestamp_cet_cest AS last_reported_cet
        , ingestion_date AS part_date_utc
        , ingestion_hour AS part_hour_utc
        , file_path AS bucket_file_path
        , loaded_at AS loaded_at_utc
        , is_returning
    FROM source_gbfs_station_status
)

SELECT
    station_id
    , num_bikes_available
    , num_docks_available
    , is_installed
    , is_renting
    , last_reported_utc
    , last_reported_cet
    , part_date_utc
    , part_hour_utc
    , bucket_file_path
    , loaded_at_utc
    , is_returning
FROM renamed
