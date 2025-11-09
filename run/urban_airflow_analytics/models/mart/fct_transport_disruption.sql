
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."fct_transport_disruption__dbt_tmp"
  
    as (
      WITH

transport_disruption AS (
    SELECT *
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."int_transport_disruption_flagged"
    WHERE 1 = 1
    -- filter by the date of pipeline stabilisation
    AND ended_at_cet >= '2025-09-29'
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
FROM transport_disruption
    );
  
  