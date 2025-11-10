{{
    config(
        materialized = 'incremental'
        , unique_key = 'date_of_day'
    )
}}

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

, french_holidays AS (
    SELECT *
    FROM
        {{ ref('int_french_holidays') }}
    WHERE
        1 = 1
        AND is_national = TRUE
)

, new_dates AS (
    SELECT *
    FROM
        dates_from_2024
    {% if is_incremental() %}
        WHERE date_of_day > (SELECT MAX(date_of_day) FROM {{ this }})
    {% endif %}
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

, add_french_holidays AS (
    SELECT
        dates_comparable_by_week.*
        , french_holidays.holiday_name_fr
        , COALESCE(french_holidays.holiday_name_fr IS NOT NULL, FALSE)
            AS is_holiday
    FROM
        dates_comparable_by_week
    LEFT JOIN french_holidays
        ON dates_comparable_by_week.date_of_day = french_holidays.holiday_date
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
    , is_holiday
    , holiday_name_fr
    , iso_week_number
    , month_number
    , quarter_number
    , created_at_utc
FROM
    add_french_holidays
