
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."dim_french_holidays__dbt_tmp"
  
    as (
      WITH

french_holidays AS (
    SELECT
        holiday_date
        , holiday_name_fr
        , is_national
    FROM
        "warehouse_prod"."main_urban_airflow_analytics"."stg_french_holidays"
)

SELECT
    holiday_date
    , holiday_name_fr
    , is_national
FROM
    french_holidays
    );
  
  