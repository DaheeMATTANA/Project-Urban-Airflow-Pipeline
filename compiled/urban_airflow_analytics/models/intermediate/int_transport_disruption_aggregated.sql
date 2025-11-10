WITH

transport_disruption AS (
    SELECT *
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."int_transport_disruption_flagged"
)

, dim_calendar AS (
    SELECT date_of_day
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."dim_calendar"
)

, active_days AS (
    SELECT
        transport_disruption.*
        , dim_calendar.date_of_day
    FROM
        dim_calendar
    LEFT JOIN transport_disruption
        ON
            dim_calendar.date_of_day
            BETWEEN CAST(transport_disruption.started_at_cet AS DATE)
            AND CAST(transport_disruption.ended_at_cet AS DATE)
)

, disruption_daily AS (
    SELECT
        date_of_day
        , COUNT(DISTINCT event_id)
            AS num_active_disruptions
        , COUNT(DISTINCT event_id) FILTER (WHERE is_planned = TRUE)
            AS num_planned_disruptions
        , MEDIAN(DISTINCT disruption_duration)
            AS median_disruption_duration
        , COUNT(DISTINCT event_id) FILTER (WHERE duration_category = 'short')
            AS num_short_disruptions
        , COUNT(DISTINCT event_id) FILTER (WHERE duration_category = 'medium')
            AS num_medium_disruptions
        , COUNT(DISTINCT event_id) FILTER (WHERE duration_category = 'long')
            AS num_long_disruptions
        , COUNT(DISTINCT event_id) FILTER (WHERE duration_category = 'extended')
            AS num_extended_disruptions
    FROM
        active_days
    WHERE
        1 = 1
        AND duration_category != 'persistent'
    GROUP BY
        date_of_day
)

SELECT
    date_of_day
    , num_active_disruptions
    , num_planned_disruptions
    , median_disruption_duration
    , num_short_disruptions
    , num_medium_disruptions
    , num_long_disruptions
    , num_extended_disruptions
FROM disruption_daily