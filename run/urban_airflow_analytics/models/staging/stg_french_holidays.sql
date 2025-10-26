
  
    
    

    create  table
      "warehouse_prod"."main_urban_airflow_analytics"."stg_french_holidays__dbt_tmp"
  
    as (
      WITH

source_french_holidays AS (
    SELECT *
    FROM "dev_db"."raw"."raw_french_holidays"
)

, renamed AS (
    SELECT
        date AS holiday_date
        , holiday_name AS holiday_name_fr
        , is_national
        , ingestion_date AS part_date_utc
        , ingestion_hour AS part_hour_utc
        , created_at AS created_at_utc
    FROM source_french_holidays
)

SELECT
    holiday_date
    , holiday_name_fr
    , is_national
    , part_date_utc
    , part_hour_utc
    , created_at_utc
FROM renamed
    );
  
  