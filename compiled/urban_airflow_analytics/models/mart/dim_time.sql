WITH

hours_of_day AS (
    SELECT *
    FROM
        GENERATE_SERIES(0, 23) AS t (hour_number)
)

, hours_enriched AS (
    SELECT
        hour_number
        , LPAD(hour_number::TEXT, 2, '0') || 'h'
            AS hour_label
        , LPAD(hour_number::TEXT, 2, '0') || ':00'
            AS time_format
        , hour_number BETWEEN 6 AND 11
            AS is_morning
        , hour_number BETWEEN 12 AND 17
            AS is_afternoon
        , hour_number BETWEEN 18 AND 23
            AS is_evening
        , hour_number < 6
            AS is_night
        , CASE
            WHEN hour_number < 12
                THEN 'AM'
            ELSE 'PM'
        END AS am_pm
    FROM
        hours_of_day
)

SELECT
    hour_number
    , hour_label
    , time_format
    , is_morning
    , is_afternoon
    , is_evening
    , is_night
    , am_pm
FROM
    hours_enriched