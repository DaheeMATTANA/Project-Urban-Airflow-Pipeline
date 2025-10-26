

-- noqa: disable=RF02
-- noqa: disable=RF04

WITH

dates_from_2024 AS (
    SELECT *
    FROM
        GENERATE_SERIES(
            DATE '2024-01-01'
            , DATE_TRUNC('year', CURRENT_DATE)
            + INTERVAL '4 year'
            - INTERVAL '1 day'
            , INTERVAL 1 DAY
        ) AS t (date_of_day)
)

, new_dates AS (
    SELECT *
    FROM
        dates_from_2024
    
        WHERE date_of_day > (SELECT MAX(date_of_day) FROM "warehouse_prod"."main_urban_airflow_analytics"."dim_calendar")
    
)

, dates_enriched AS (
    SELECT
        *
        -- Date comparable : date Y-1 with the same week number and weekday name
        , date_of_day - INTERVAL '52 weeks' AS date_of_day_comparable
        -- with ISO format, Monday is the first day of the week 
        , STRFTIME(DATE_TRUNC('year', date_of_day), '%Y') AS year
        , DATE_TRUNC('week', date_of_day) AS iso_first_date_of_week
        , DATE_TRUNC('week', date_of_day)
        + INTERVAL 6 DAY AS iso_last_date_of_week
        , STRFTIME(date_of_day, '%A') AS weekday_name_en
        , EXTRACT(ISODOW FROM date_of_day) AS iso_day_of_week
        , COALESCE(
            EXTRACT(ISODOW FROM date_of_day) = 6
            OR EXTRACT(ISODOW FROM date_of_day) = 7
            , FALSE
        ) AS is_weekend
        , EXTRACT(WEEK FROM date_of_day) AS iso_week_number
        , EXTRACT(MONTH FROM date_of_day) AS month_number
        , EXTRACT(QUARTER FROM date_of_day) AS quarter_number
    FROM
        new_dates
)

, dates_comparable_by_week AS (
    SELECT
        *
        , STRFTIME(DATE_TRUNC('year', date_of_day_comparable), '%Y')
            AS year_comparable
        , DATE_TRUNC('week', date_of_day_comparable)
            AS iso_first_date_of_week_comparable
        , DATE_TRUNC('week', date_of_day_comparable)
        + INTERVAL 6 DAY AS iso_last_date_of_week_comparable
        , CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS created_at_utc
    FROM
        dates_enriched
)

SELECT
    date_of_day
    , date_of_day_comparable
    , year
    , year_comparable
    , iso_first_date_of_week
    , iso_first_date_of_week_comparable
    , iso_last_date_of_week
    , iso_last_date_of_week_comparable
    , weekday_name_en
    , iso_day_of_week
    , is_weekend
    , iso_week_number
    , month_number
    , quarter_number
    , created_at_utc
FROM
    dates_comparable_by_week