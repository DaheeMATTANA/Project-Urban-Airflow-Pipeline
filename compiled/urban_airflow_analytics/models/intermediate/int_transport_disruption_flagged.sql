WITH

transport_disruption AS (
    SELECT
        event_id
        , started_at_cet
        , ended_at_cet
        , disruption_cause_fr
        , disruption_severity_fr
        , message_title_fr
        , message_body_fr
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."stg_transport_disruption"
)

, disruption_flags AS (
    SELECT
        event_id
        , started_at_cet
        , ended_at_cet
        , disruption_cause_fr
        , disruption_severity_fr
        , message_title_fr
        , message_body_fr
        , ended_at_cet - started_at_cet
            AS disruption_duration
        , COALESCE(ended_at_cet > CURRENT_TIMESTAMP, FALSE)
            AS is_active
        , COALESCE(started_at_cet > CURRENT_TIMESTAMP, FALSE)
            AS is_planned
    FROM
        transport_disruption
)

, duration_flags AS (
    SELECT
        event_id
        , started_at_cet
        , ended_at_cet
        , disruption_cause_fr
        , disruption_severity_fr
        , message_title_fr
        , message_body_fr
        , disruption_duration
        , is_active
        , is_planned
        , CASE
            WHEN disruption_duration < '1 hour'
                THEN 'short'
            WHEN disruption_duration BETWEEN '1 hour' AND '24 hours'
                THEN 'medium'
            WHEN disruption_duration BETWEEN '1 day' AND '7 days'
                THEN 'long'
            WHEN disruption_duration BETWEEN '7 days' AND '30 days'
                THEN 'extended'
            ELSE 'persistent'
        END AS duration_category
    FROM
        disruption_flags
)

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
FROM
    duration_flags