



select
    1
from "warehouse_prod"."main_urban_airflow_analytics"."fct_transport_disruption"

where not(started_at_cet < ended_at_cet)

