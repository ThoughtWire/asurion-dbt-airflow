{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', 'id', 'cluster'],
    incremental_strategy = 'delete+insert', 
    indexes = [{'columns': ['timestamp'],
    'type': 'btree' } ], tags = ['lutron']
) }} 

{% if is_incremental() %}

{% set max_time = get_max_value('insights_lutron', 'timestamp', 1) %}

{% endif %}

WITH filtered_metric AS (

    SELECT 
        id,
        cluster,
        value,
        time_bucket('1 hour', timestamp at time zone 'America/Chicago')::timestamp as timestamp  
    FROM {{source('raw_data_prod', 'metric')}} 
    WHERE id IN ('Light Level Discrepancy', 'Lighting Level', 'Lighting Power Used', 
                'Loadshed Allowed', 'Loadshed Goal', 'Power Savings By Daylighting',
                'Power Savings By Loadshedding', 'Power Savings By Occupancy/Vacancy',
                'Power Savings By Personal Control', 'Power Savings By Schedules', 
                'Power Savings By Tuning') 

    {% if is_incremental() %}

        and (timestamp) > {{ max_time }}::timestamp - interval '2 days'

    {% endif %}
),
agg as (
    SELECT 
        id, 
        cluster, 
        timestamp,
        avg(value) as avg, 
        sum(value) as sum
    FROM filtered_metric
    GROUP BY id, cluster, timestamp
),

date_calc AS (
    SELECT 
        *,
        timestamp::DATE as timestampDate,
        dense_rank() OVER (ORDER BY timestamp::DATE DESC) as day_order,
        date_part('dow', timestamp)::int as dow,
        date_part('year', timestamp)::int as yr,
        date_part('month', timestamp)::int as mnth,
        date_part('hour', timestamp)::int as hr
    FROM agg
)
SELECT
    d.id,
    d.avg,
    d.sum,
    d.cluster,
    d.timestamp,
    d.timestampDate,
    trim(to_char(d.timestamp, 'Day'))::text as "timestampDayOfWeekName",
    trim(to_char(d.timestamp, 'Month'))::text as "timestampMonthName",
    d.dow as "timestampDayOfWeek",
    d.yr as "timestampYear",
    d.mnth as "timestampMonth",
    d.hr as "timestampHour",
    trim(to_char(d.timestamp, 'mm yyyy'))::text as "MMYYDate",
    TO_CHAR(d.timestamp, 'FMHH12 AM') as "Time AM/PM",
    CASE WHEN d.id like '%Power Savings%' THEN 1 ELSE 0 END as "Power Saving",
    CASE 
        WHEN d.day_order = 1 THEN 'Today'
        WHEN d.day_order = 2 THEN 'Yesterday'
        ELSE 'NA'
    END AS "Today",
    CASE 
        WHEN d.hr BETWEEN 9 AND 17 THEN 'Working Hours'
        ELSE 'Off Hours'
    END AS "Working Hours",
    pz.*,
    'lutron' as vendorname
FROM date_calc d
LEFT JOIN {{ref('pb_zones')}} pz 
    ON pz.external_id = d.cluster 
    AND pz.cluster_relationship_type = 200 
    AND pz.bldgcode = 'NGHB'