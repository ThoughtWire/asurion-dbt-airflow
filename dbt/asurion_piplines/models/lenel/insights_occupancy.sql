{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', 'cluster'], 
    strategy = 'delete+insert',
    indexes = [{'columns': ['timestamp'],
    'type': 'btree' } ], tags = ['vergesense']
) }} 


{% if is_incremental() %}

{% set max_time = get_max_value('insights_occupancy', 'timestamp') %}

{% endif %}

WITH filtered_metric AS (
    SELECT
        id,
        cluster,
        value,
        timestamp AT TIME ZONE 'America/Chicago'::timestamp as timestamp 
    FROM {{source('raw_data_prod', 'metric')}}
    WHERE id = 'peopleCount'  
    {% if is_incremental() %}
        AND timestamp > {{ max_time }}::timestamp - interval '2 days'
    {% endif %}
),

bucketed_data AS (
    SELECT 
        'peopleCount' as id,
        cluster,
        value,
        CASE WHEN value > 0 THEN 1 ELSE 0 END AS occ,
        time_bucket('1 hour', timestamp) as timestamp,
        timestamp::DATE as timestamp_date,
        date_part('hour', timestamp)::int as hour,
        date_part('dow', timestamp)::int as dow,
        date_part('year', timestamp)::int as yr,
        date_part('month', timestamp)::int as mnth
    FROM filtered_metric
),

aggregated_data AS (
    SELECT 
        id,
        cluster,
        timestamp,
        timestamp_date,
        hour,
        dow,
        yr,
        mnth,
        avg(value) as avg,
        sum(value) as sum,
        max(value) as max,
        min(value) as min,
        sum(occ) as mins,
        dense_rank() OVER (ORDER BY timestamp_date DESC) as day_order
    FROM bucketed_data
    GROUP BY 
        id, cluster, timestamp, timestamp_date, hour, dow, yr, mnth
)

SELECT
    ad.id,
    ad.avg,
    ad.sum,
    ad.min,
    ad.max,
    ad.mins,
    ad.cluster,
    ad.timestamp,
    ad.timestamp_date as "timestampDate",
    trim(to_char(ad.timestamp, 'Day'))::text as "timestampDayOfWeekName",
    trim(to_char(ad.timestamp, 'Month'))::text as "timestampMonthName",
    ad.dow as "timestampDayOfWeek",
    ad.yr as "timestampYear",
    ad.mnth as "timestampMonth",
    ad.hour as "timestampHour",
    TO_CHAR(ad.timestamp, 'Mon-YYYY') as "MMYYYY",
    CASE 
        WHEN ad.day_order = 1 THEN 'Today'
        WHEN ad.day_order = 2 THEN 'Yesterday'
        ELSE 'NA'
    END AS "Today",
    CASE
        WHEN ad.hour BETWEEN 9 AND 17 THEN 'Working Hours'
        ELSE 'Off Hours'
    END AS "Working Hours",
    pz."Floor",
    pz."Floor#",
    pz."Building" as "BuildingName",
    pz."Room Type",
    pz."room" as "zone_name",
    pz.capacity as "Capacity",
    pz.svp_occpln,
    'vergesense' as vendorname
FROM aggregated_data ad
LEFT JOIN {{ref('pb_zones')}} pz 
    ON pz.external_id = ad.cluster 
    AND pz.bldgcode = 'NGHB'
    AND pz.subsystem = 'vergesense'

