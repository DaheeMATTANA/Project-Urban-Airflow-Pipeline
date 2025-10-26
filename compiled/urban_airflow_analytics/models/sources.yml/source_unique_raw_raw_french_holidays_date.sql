
    
    

select
    date as unique_field,
    count(*) as n_records

from "dev_db"."raw"."raw_french_holidays"
where date is not null
group by date
having count(*) > 1


