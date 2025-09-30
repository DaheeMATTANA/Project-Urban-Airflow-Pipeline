WITH

source_open_meteo AS (
    SELECT
        *
    FROM {{ source('raw', 'raw_open_meteo') }}
)

, renamed AS (
    SELECT
        time AS timestamp_utc
        , time_cet AS timestamp_cet
        , temperature_2m
        , precipitation
        , precipitation_probability
        , visibility
        , windspeed_10m
        , ingestion_date AS part_date_utc
        , ingestion_hour AS part_hour_utc
        , created_at AS loaded_at_utc
    FROM source_open_meteo
)

SELECT
    timestamp_utc
    , timestamp_cet
    , temperature_2m
    , precipitation
    , precipitation_probability
    , visibility
    , windspeed_10m
    , part_date_utc
    , part_hour_utc
    , loaded_at_utc
FROM renamed
