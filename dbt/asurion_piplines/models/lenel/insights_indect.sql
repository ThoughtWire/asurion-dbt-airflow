{{ config(materialized = 'incremental', 
    unique_key = ['timestamp', '"ZoneName"'],
    incremental_strategy = 'delete+insert', 
    indexes = [{'columns': ['timestamp'],
    'type': 'btree' } ], tags = ['indect']
)
}} 


{% if is_incremental() %} 
{% set max_time = get_max_value('insights_indect', 'timestamp') %} 
{% endif %} 

with parsed_payload as (
        select
            timestamp at time zone 'America/Chicago' as timestamp,
            "Zones" as pzones
        from
            {{ source('raw_data_prod', 'audit') }},
            jsonb_to_recordset(audit.payload) as items("Zones" jsonb)
        where
            vendor = 'indect' 
            
            {% if is_incremental() %}

            and timestamp > {{ max_time }}::timestamp - interval '2 months' 
            
            {% endif %}
    ),

    parsed_data as (
        select
            timestamp,
            "Zones" as zones
        from
            parsed_payload,
            jsonb_to_recordset(parsed_payload.pzones) as items("Zones" jsonb)
    ),

    final_data as (
        select
            timestamp,
            
            dense_rank() over (order by timestamp :: DATE desc) as day_order,
            dense_rank() over (order by date_part('year', timestamp) desc, date_part('month', timestamp) desc) as month_order,
            dense_rank() over (order by date_part('year', timestamp) desc) as year_order,
            

            (
                jsonb_populate_recordset(null :: indect, zones :: jsonb)
            ).*
        from
            parsed_data
    ),

    duplicates_removed as (

        select *, row_number() over (partition by "Name", timestamp order by timestamp desc) as rn_number
        from final_data
    ),

    final_result as (

        select timestamp, 
                timestamp :: DATE as "timestampDate",
                trim(to_char(timestamp, 'Day')) :: text as "WeekDayName",
                trim(to_char(timestamp, 'Month')) :: text as "timestampMonthName",
                date_part('dow', timestamp) :: int as "WeekDay",
                date_part('year', timestamp) :: int as "timestampYear",
                date_part('month', timestamp) :: int as "timestampMonth",
                date_part('hour', timestamp) :: int as "timestampHour",
                timestamp :: TIME as "timestampTime",
                TO_CHAR(timestamp, 'FMHH12 AM') as "Hour-AMPM",
                to_date(trim(to_char(timestamp, 'Mon yyyy')), 'Mon yyyy') as  "MMYYDate",

                1863::int as "TotalBays", 
                'Indect' as vendor,
                CASE 
                WHEN "Name" = 'Level 1' THEN 'P1'
                WHEN "Name" = 'Level 2' THEN 'P2'
                WHEN "Name" = 'Level 3' THEN 'P3'
                WHEN "Name" = 'Level 4' THEN 'P4'
                WHEN "Name" = 'Level 5' THEN 'P5'
                ELSE 'P6'
                END as "ZoneName",
                "TotalBays" as "ZoneTotalBays", 
                "OccupiedBays" as "ZoneOccupiedBays", 
                "TotalBays" - "OccupiedBays" as "UnusedBays",
                ("OccupiedBays" / "TotalBays") as "%Occupancy",

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

            CONCAT(trim(to_char(timestamp, 'Day')) , ', ', TO_CHAR(timestamp, 'FMHH12 AM')) as "Day&Hour"
            
            from duplicates_removed
            where rn_number = 1
    )

select
    *
from
    final_result