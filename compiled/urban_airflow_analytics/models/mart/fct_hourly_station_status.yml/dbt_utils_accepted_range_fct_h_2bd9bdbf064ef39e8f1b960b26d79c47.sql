

with meet_condition as(
  select *
  from "warehouse_prod"."main_urban_airflow_analytics"."fct_hourly_station_status"
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not last_reported_hour_cet >= 0
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not last_reported_hour_cet <= 23
)

select *
from validation_errors

