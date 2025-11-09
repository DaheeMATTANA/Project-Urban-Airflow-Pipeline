
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."int_transport_disruption_flagged"
where event_id is not null
group by event_id
having count(*) > 1


