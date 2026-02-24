SELECT
    station_id
FROM 
    "warehouse_prod"."main_urban_airflow_analytics"."dim_station"
WHERE
    is_current
GROUP BY
    station_id
HAVING 1 = 1
    AND COUNT(*) > 1