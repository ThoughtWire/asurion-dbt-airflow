{{ config(materialized = 'ephemeral', tags = ['zones']) }} 



WITH floors AS (
    SELECT
        *
    FROM
        zone
    WHERE
        zone_type = 105
),

modified_floors AS (
    SELECT
        zone_id, 
        CASE 
            WHEN substring(zone_name, 1, 1) = '0' THEN right(zone_name, 1)
            ELSE zone_name
        END AS zone_name, 
        zone_position, 
        zone_type
    FROM
        floors
)

SELECT
    *
FROM
    modified_floors
