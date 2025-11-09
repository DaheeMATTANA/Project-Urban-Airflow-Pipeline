WITH

hourly_weather AS (
    SELECT
        timestamp_cet
        , temperature_2m
        , precipitation
        , visibility
        , windspeed_10m
        , general_weather_condition
        , temperature_category
        , precipitation_category
        , precipitation_probability_category
        , wind_category
        , visibility_category
        , anormality_category
        , precipitation_probability * 0.01
            AS precipitation_probability
    FROM
        {{ ref('int_hourly_weather_flagged') }}
    WHERE 1 = 1
    -- filter by the date of pipeline stabilisation
    AND timestamp_cet >= '2025-09-29'
)

SELECT
    timestamp_cet
    , temperature_2m
    , precipitation
    , precipitation_probability
    , visibility
    , windspeed_10m
    , general_weather_condition
    , temperature_category
    , precipitation_category
    , precipitation_probability_category
    , wind_category
    , visibility_category
    , anormality_category
FROM
    hourly_weather
