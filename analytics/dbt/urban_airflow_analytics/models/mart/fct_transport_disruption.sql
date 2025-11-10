WITH

transport_disruption AS (
    SELECT *
    FROM
        {{ ref('int_transport_disruption_flagged') }}
    WHERE 1 = 1
    -- filter by the date of pipeline stabilisation
    AND ended_at_cet >= '2025-09-29'
)

, add_date_columns AS (
    SELECT
        event_id
        , started_at_cet
        , ended_at_cet
        , disruption_cause_fr
        , disruption_severity_fr
        , message_title_fr
        , message_body_fr
        , disruption_duration
        , duration_category
        , is_active
        , is_planned
        , CAST(started_at_cet AS DATE) AS started_date_cet
        , HOUR(started_at_cet) AS started_hour_cet
        , CAST(ended_at_cet AS DATE) AS ended_date_cet
        , HOUR(ended_at_cet) AS ended_hour_cet
    FROM
        transport_disruption
)

SELECT
    event_id
    , started_at_cet
    , started_date_cet
    , started_hour_cet
    , ended_at_cet
    , ended_date_cet
    , ended_hour_cet
    , disruption_cause_fr
    , disruption_severity_fr
    , message_title_fr
    , message_body_fr
    , disruption_duration
    , duration_category
    , is_active
    , is_planned
FROM add_date_columns
