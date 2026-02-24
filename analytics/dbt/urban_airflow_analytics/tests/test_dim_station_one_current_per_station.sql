SELECT
    station_id
FROM 
    {{ ref('dim_station') }}
WHERE
    is_current
GROUP BY
    station_id
HAVING 1 = 1
    AND COUNT(*) > 1