
    
    

select
    date_cet as unique_field,
    count(*) as n_records

from "warehouse_prod"."main_urban_airflow_analytics"."int_transport_disruption_aggregated"
where date_cet is not null
group by date_cet
having count(*) > 1


