
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."int_hourly_weather_flagged"
  
    as (
      

-- noqa: disable=RF02

WITH

open_meteo AS (
    SELECT
        timestamp_utc
        , temperature_2m
        , precipitation
        , precipitation_probability
        , visibility
        , windspeed_10m
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."stg_open_meteo"
    
)

, weather_flags AS (
    SELECT
        timestamp_utc
        , temperature_2m
        , precipitation
        , precipitation_probability
        , visibility
        , windspeed_10m
        , timestamp_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Paris'
            AS timestamp_cet
        , CASE
            WHEN
                precipitation > 0.1
                AND temperature_2m <= 1
                THEN 'snow'
            WHEN
                precipitation > 0.1
                AND temperature_2m > 1
                THEN 'rain'
            WHEN visibility < 1000
                THEN 'fog'
            WHEN
                precipitation = 0
                AND visibility >= 10000
                THEN 'clear'
            ELSE 'cloudy'
        END AS general_weather_condition
        , CASE
            WHEN temperature_2m < 0
                THEN 'cold'
            WHEN temperature_2m < 15
                THEN 'mild'
            WHEN temperature_2m < 25
                THEN 'warm'
            ELSE 'hot'
        END AS temperature_category
        , CASE
            WHEN precipitation = 0
                THEN 'none'
            WHEN precipitation < 2
                THEN 'light'
            WHEN precipitation < 10
                THEN 'moderate'
            ELSE 'heavy'
        END AS precipitation_category
        , CASE
            WHEN precipitation_probability < 20
                THEN 'low'
            WHEN precipitation_probability < 60
                THEN 'medium'
            ELSE 'high'
        END AS precipitation_probability_category
        , CASE
            WHEN windspeed_10m < 2
                THEN 'calm'
            WHEN windspeed_10m < 8
                THEN 'breezy'
            WHEN windspeed_10m < 17
                THEN 'windy'
            ELSE 'storm'
        END AS wind_category
        , CASE
            WHEN visibility < 1000
                THEN 'poor'
            WHEN visibility < 10000
                THEN 'moderate'
            ELSE 'good'
        END AS visibility_category
        , CASE
            WHEN
                temperature_2m < -40
                OR temperature_2m > 50
                OR precipitation > 50
                OR windspeed_10m > 40
                OR visibility < 0
                OR visibility > 150000
                THEN 'very anormal'
            WHEN
                temperature_2m < -30
                OR temperature_2m > 45
                OR precipitation > 20
                OR windspeed_10m > 25
                OR visibility < 100
                OR visibility > 100000
                THEN 'anormal'
            ELSE 'none'
        END AS anormality_category
    FROM
        open_meteo
)

SELECT
    timestamp_utc
    , timestamp_cet
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
    weather_flags
    );
  
  
  