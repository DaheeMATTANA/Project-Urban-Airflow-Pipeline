WITH

french_holidays AS (
    SELECT
        holiday_date
        , hoilday_name_fr
        , is_national
    FROM
        {{ ref('stg_french_holidays') }}
)

SELECT
    holiday_date
    , hoilday_name_fr
    , is_national
FROM
    french_holidays
