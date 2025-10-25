SELECT
    station_id
FROM 
    {{ ref('dim_station') }}
GROUP BY
    station_id
HAVING 1 = 1
    AND COUNT(*) FILTER (WHERE is_current) != 1