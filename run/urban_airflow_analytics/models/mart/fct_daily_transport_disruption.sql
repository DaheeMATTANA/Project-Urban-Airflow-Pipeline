
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."fct_daily_transport_disruption__dbt_tmp"
  
    as (
      WITH

transport_disruption AS (
    SELECT *
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."int_transport_disruption_aggregated"
    WHERE 1 = 1
    -- filter by the date of pipeline stabilisation
    AND date_cet >= '2025-09-29'
)

SELECT
    date_cet
    , num_active_disruptions
    , num_planned_disruptions
    , median_disruption_duration
    , num_short_disruptions
    , num_medium_disruptions
    , num_long_disruptions
    , num_extended_disruptions
    , num_persistent_disruptions
    , num_construction_disruptions
    , num_incident_disruptions
    , num_blocking_disruptions
    , num_delayed_disruptions
FROM transport_disruption
    );
  
  