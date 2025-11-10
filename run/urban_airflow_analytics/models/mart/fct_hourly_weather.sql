
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_weather__dbt_tmp"
  
    as (
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
        , precipitation_probability
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."int_hourly_weather_flagged"
    WHERE 1 = 1
    -- filter by the date of pipeline stabilisation
    AND timestamp_cet >= '2025-09-29'
)

, add_date_columns AS (
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
        , CAST(timestamp_cet AS DATE) AS date_cet
        , HOUR(timestamp_cet) AS hour_cet
        , precipitation_probability * 0.01
            AS precipitation_probability
    FROM
        hourly_weather
)

SELECT
    timestamp_cet
    , date_cet
    , hour_cet
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
    add_date_columns
    );
  
  