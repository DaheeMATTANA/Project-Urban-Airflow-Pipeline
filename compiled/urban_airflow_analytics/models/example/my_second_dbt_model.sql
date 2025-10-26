-- Use the `ref` function to select from other models

select *
from "warehouse_prod"."main"."my_first_dbt_model"
where id = 1