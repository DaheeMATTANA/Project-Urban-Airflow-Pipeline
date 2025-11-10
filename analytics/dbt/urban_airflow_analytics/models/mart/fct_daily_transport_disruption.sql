WITH

transport_disruption AS (
    SELECT *
    FROM
        {{ ref('int_transport_disruption_aggregated') }}
    WHERE 1 = 1
    -- filter by the date of pipeline stabilisation
    AND date_of_day >= '2025-09-29'
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
FROM transport_disruption
