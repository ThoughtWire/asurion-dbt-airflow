{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', 'id', 'cluster'],
    incremental_strategy = 'delete+insert', 
    indexes = [{'columns': ['timestamp'],
    'type': 'btree' } ], tags = ['alerton']
) }} 


{% if is_incremental() %}

{% set max_time = get_max_value('insights_alerton', 'timestamp') %}

{% endif %}

WITH filtered_metric AS (
    SELECT
        id,
        cluster,
        value,
        (timestamp at time zone 'America/Chicago') as "timestamp" 
    FROM
        {{source('raw_data_prod', 'metric')}}
    where
        id = ANY ( ARRAY[
            'Setpoint (SP)',
            'Space Humidity',
            'CO2 ppm',
            'Current Cooling SP',
            'Current Heating SP',
            'Microset Room Temp'])
            
        {% if is_incremental() %}

           and timestamp > {{ max_time }}::timestamp - interval '2 days'

        {% endif %}
), 



bucketing as (

		select id, cluster, value,
		time_bucket('1 hour', "timestamp") as timestamp	
		from filtered_metric

),

agg as (
		select id, cluster, timestamp, 
		avg(value) as avg, sum(value) as sum, max(value) as max, min(value) as min
		from bucketing
		group by id, cluster, timestamp

),

final_data as (

            select *, 

            dense_rank() over (order by timestamp :: DATE desc) as day_order
            -- dense_rank() over (order by date_part('year', timestamp) desc, date_part('month', timestamp) desc) as month_order,
            -- dense_rank() over (order by date_part('year', timestamp) desc) as year_order
           
            from agg
),


final_result as (
    select
        fd.id,
        fd.avg,
        fd.sum,
        fd.min,
        fd.max,
        cluster,
        split_part(cluster, '/', 3) as "Device type",
        timestamp,
        timestamp :: DATE as "timestampDate",
        trim(to_char(timestamp, 'Day')) :: text as "timestampDayOfWeekName",
        trim(to_char(timestamp, 'Month')) :: text as "timestampMonthName",
        date_part('dow', timestamp) :: int as "timestampDayOfWeek",
        date_part('year', timestamp) :: int as "timestampYear",
        date_part('month', timestamp) :: int as "timestampMonth",
        date_part('hour', timestamp) :: int as "timestampHour",

        -- CASE
        --     WHEN day_order <= 30 THEN 'Last 30 Days'
        --     Else 'NA' 
        -- END AS "Last 30 Days", 

        CASE
            WHEN day_order = 1 THEN 'Today'
            WHEN day_order = 2 THEN 'Yesterday'
        Else 'NA' 
        END AS "Today",

        -- CASE
        --     WHEN year_order = 1 THEN 'YTD'
        -- Else 'NA' 
        -- END AS "YearLabel",

        -- CASE
        --     WHEN month_order = 1 THEN 'Current Month'
        --     WHEN month_order = 2 THEN 'Previous Month'
        -- Else 'NA' 
        -- END AS "Month_label", 

         CASE
                WHEN date_part('hour', timestamp) >= 9 and date_part('hour', timestamp) <= 17 THEN 'Working Hours'
                Else 'Off Hours' 

        END AS "Working Hours",



        COALESCE(pz."Floor", 'Asurion') as "Floor", COALESCE(pz."Floor#", 'Asurion') as "Floor#", COALESCE(pz."Building", 'Asurion') as "Building", 
        COALESCE(pz."Room Type", 'Asurion') as "Room Type", COALESCE(pz."room", 'Asurion') as "zone_name",
        'alerton' as vendorname
    from
        final_data fd
        
        left join {{ref('pb_zones')}} pz on (pz.external_id = fd."cluster")
    where pz.cluster_relationship_type = 201
)
select
    *
from
    final_result