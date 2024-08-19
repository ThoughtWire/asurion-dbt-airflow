{{ config(
    materialized = 'incremental',
    indexes = [{'columns': ['timestamp'],
    'type': 'btree' } ], tags = ['lutron']
) }} 

{% if is_incremental() %}

{% set max_time = get_max_value('insights_lutron', 'timestamp', 1) %}

{% endif %}

WITH filtered_metric AS (

    select 
        id,
        cluster,
        value,
        (timestamp at time zone 'America/Chicago') as "timestamp"  

        from {{source('raw_data_prod', 'metric')}} 

    where
        id = ANY (ARRAY['Light Level Discrepancy','Lighting Level','Lighting Power Used','Loadshed Allowed',
        'Loadshed Goal','Power Savings By Daylighting','Power Savings By Loadshedding',
		'Power Savings By Occupancy/Vacancy','Power Savings By Personal Control',
        'Power Savings By Schedules','Power Savings By Tuning'])

        {% if is_incremental() %}

           and (timestamp) > {{ max_time }}

        {% endif %}
), 


bucketing as (

		select id, cluster, value,
		time_bucket('1 hour', "timestamp") as timestamp	
		from filtered_metric

),

agg as (
		select id, cluster, timestamp, 
		avg(value) as avg, sum(value) as sum
		from bucketing
		group by id, cluster, timestamp

),


final_data as (

            select *, 

            dense_rank() over (order by timestamp :: DATE desc) as day_order,
            dense_rank() over (order by date_part('year', timestamp) desc, date_part('month', timestamp) desc) as month_order,
            dense_rank() over (order by date_part('year', timestamp) desc) as year_order
           
            from agg
),


final_result as (
    select
        fd.id,
        fd.avg,
        fd.sum,
        cluster,
        "timestamp",
        timestamp :: DATE as "timestampDate",
        trim(to_char(timestamp, 'Day')) :: text as "timestampDayOfWeekName",
        trim(to_char(timestamp, 'Month')) :: text as "timestampMonthName",
        date_part('dow', timestamp) :: int as "timestampDayOfWeek",
        date_part('year', timestamp) :: int as "timestampYear",
        date_part('month', timestamp) :: int as "timestampMonth",
        date_part('hour', timestamp) :: int as "timestampHour",
        trim(to_char(timestamp, 'mm yyyy')) :: text as "MMYYDate",
        TO_CHAR(timestamp, 'FMHH12 AM') as "Time AM/PM",

        CASE 
            
            WHEN fd.id like '%Power Savings%' THEN 1
            ELSE 0
        END as "Power Saving",

        CASE
            WHEN day_order <= 30 THEN 'Last 30 Days'
            Else 'NA' 
        END AS "Last 30 Days", 

        CASE
            WHEN day_order = 1 THEN 'Today'
            WHEN day_order = 2 THEN 'Yesterday'
        Else 'NA' 
        END AS "Today",

        CASE
            WHEN year_order = 1 THEN 'YTD'
        Else 'NA' 
        END AS "YearLabel",

        CASE
            WHEN month_order = 1 THEN 'Current Month'
            WHEN month_order = 2 THEN 'Previous Month'
        Else 'NA' 
        END AS "Month_label", 

         CASE
                WHEN date_part('hour', timestamp) >= 9 and date_part('hour', timestamp) <= 17 THEN 'Working Hours'
                Else 'Off Hours' 

        END AS "Working Hours",
        

        pz.*,
        'lutron' as vendorname
    from
        final_data fd
        
        left join {{ref('pb_zones')}} pz on (pz.external_id = fd."cluster")
)

select
    *
from
    final_result