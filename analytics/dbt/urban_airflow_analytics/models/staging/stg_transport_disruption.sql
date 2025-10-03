WITH

source_transport_disruption AS (
    SELECT *
    FROM {{ source('raw', 'raw_transport_disruption') }}
)

, renamed AS (
    SELECT
        event_id
        , start_ts AS started_at_cet
        , end_ts AS ended_at_cet
        , cause AS disruption_cause_fr
        , severity AS disruption_severity_fr
        , title AS message_title_fr
        , message AS message_body_fr
        , ingestion_date AS part_date_utc
        , ingestion_hour AS part_hour_utc
        , created_at AS loaded_at_utc
    FROM source_transport_disruption
)

SELECT
    event_id
    , started_at_cet
    , ended_at_cet
    , disruption_cause_fr
    , disruption_severity_fr
    , message_title_fr
    , message_body_fr
    , part_date_utc
    , part_hour_utc
    , loaded_at_utc
FROM renamed
