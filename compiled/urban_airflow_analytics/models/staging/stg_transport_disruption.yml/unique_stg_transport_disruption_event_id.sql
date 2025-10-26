
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."stg_transport_disruption"
where event_id is not null
group by event_id
having count(*) > 1


