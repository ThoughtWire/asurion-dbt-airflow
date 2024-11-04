{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', '"Available"', '"Occupied"', '"Reserved"'],
    strategy = 'delete+insert',
    indexes = [{'columns': ['timestamp'],
    'type': 'btree' }], tags = ['luxerone']
) }} 


 {% if is_incremental() %}

{% set max_time = get_max_value('insights_luxerone', 'timestamp') %}

{% endif %}


WITH parsed_data AS (

            select distinct 
            timestamp at time zone 'America/Chicago' as timestamp,
            (
                jsonb_populate_recordset(null :: luxerone, payload :: jsonb)
            ).*
        from
            {{ source('raw_data_prod', 'audit') }}

        where
            subsystem = 'lockerAudit' 
            
            {% if is_incremental() %}

                and timestamp > {{max_time}}::timestamp - interval '2 months'
            
            {% endif %}

), 

final_data as (

            select locker_type_id as "Locker_Type_ID",  type as "Type", occupied as "Occupied", 
            out_of_service as "Out_Of_Service", reserved as "Reserved", available as "Available", timestamp,

            dense_rank() over (order by timestamp :: DATE desc) as day_order,
            dense_rank() over (order by date_part('year', timestamp) desc, date_part('month', timestamp) desc) as month_order,
            dense_rank() over (order by date_part('year', timestamp) desc) as year_order
           
            from parsed_data
),

final_result as (

    select
        timestamp,"Available", "Occupied", "Reserved", "Out_Of_Service",
        Null as "Locker Size", Null as "DeliveryID", NULL::timestamp as "TimeOfPickUpUTC",
        NULL::int as "Pick Up Duration",
        Null as "Pick up Duration Comparison", "Type", 
        NULL::int as "Held in Lockers", NULL::int as "picked up", 

        CASE 
            WHEN "Type"='Small' THEN 1
            WHEN "Type"='Medium' THEN 2
            ELSE 3
        END as "LockerType#",

        timestamp :: DATE as "TimestampDate",
        trim(to_char(timestamp, 'Day')) :: text as "Day",
        trim(to_char(timestamp, 'Month')) :: text as "Month",
        date_part('dow', timestamp) :: int as "Dayofweek#",
        date_part('year', timestamp) :: int as "Year",
        date_part('month', timestamp) :: int as "Month#",
        date_part('hour', timestamp) :: int as "Hour",
        to_date(trim(to_char(timestamp, 'Mon yyyy')), 'Mon yyyy') as "MMYYEventDate",

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
        0 as "TargetOutOfService",
        97 as "TotalLockers",
        0 as "Target Maintenance",
        'Lockers System' as subsystem,
        'Lockers' as "vendor"

    from
        final_data
    )

select
    *
from
    final_result