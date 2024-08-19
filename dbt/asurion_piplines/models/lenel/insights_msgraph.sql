{{ config(
    materialized = 'materialized_view',
    tags = ['msgraph']
) }} 


with release as (
    select
        *
    from
        audit
    where
        subsystem = 'room-release'
        and payload -> 0 ->> 'status' = 'emailSent'
        and "timestamp" > '2023-01-01'
),

emails as (
    select
        to_char(timestamp at time zone 'America/Chicago', 'Mon YYYY') as timestamp,
        count(*) as cnt
    from
        release
    group by
        to_char(timestamp at time zone 'America/Chicago', 'Mon YYYY')
),

historical_reservations as (
    select
        row_number() over(
            partition by r.id
            order by
                r.start_time desc
        ) as rn,
        r.id,
        array_length(guests_id, 1) as number_of_guest,
        COALESCE(z."Floor", 'NA') as "Floor",
        COALESCE(z."Floor#", 'NA') as "Floor#",
        COALESCE(z."Building", 'NA') as "BuildingName",
        z.room as zone_name,
        da.capacity as "Capacity",
        da.type_name as "TypeName",
        r.online_meeting_provider,
        r.subject,
        r.start_time,
        r.end_time,
        r.status,
        eb.details,
        ea.timestamp,
        ea.cnt
    from
        {{ source('raw_data_uat', 'reservation_history') }} r
        left join {{ ref('pb_zones') }} z on (z.zone_id = r.zone_id) 
        left join enum_booking_action eb on (eb.id = r.status)
        left join insights_ioffice_roominfo da on (da.name = z.room)
        left join emails ea on (ea.timestamp = to_char(r.start_time, 'Mon YYYY'))
    where
        r.zone_id is not null
        and subject not ilike '%test%'
        and r.start_time > '2023-01-01'
),
parsed_dates as (
    select
        *,
        start_time :: DATE as "StartTimeDate",
        trim(to_char(start_time, 'Day')) :: text as "StartTimeDayOfWeekName",
        trim(to_char(start_time, 'Month')) :: text as "StartTimeMonthName",
        date_part('dow', start_time) :: int as "StartTimeDayOfWeek",
        date_part('year', start_time) :: int as "StartTimeYear",
        date_part('month', start_time) :: int as "StartTimeMonth",
        date_part('hour', start_time) :: int as "StartTimeHour", 
        to_date(trim(to_char(start_time, 'Mon yyyy')), 'Mon yyyy') as "MMYYEventDate",

        CASE 
        
            WHEN date_part('hour', start_time) <= 2 OR date_part('hour', start_time) >= 23 THEN 'Night (11PM-2AM)'
            WHEN date_part('hour', start_time) <= 6 THEN 'Morning (3AM-6AM)'
            WHEN date_part('hour', start_time) <= 10 THEN 'Day Start (7AM-10AM)'
            WHEN date_part('hour', start_time) <= 14 THEN 'Day Mid (11AM-2PM)'
            WHEN date_part('hour', start_time) <= 18 THEN 'Day End (3PM-6PM)'
            ELSE 'Evening (6PM-10PM)'
        
        END AS "hour_binned", 
        
        dense_rank() over (order by start_time :: DATE desc) as day_order,
        dense_rank() over (order by date_part('year', start_time) desc, date_part('month', start_time) desc) as month_order,
        dense_rank() over (order by date_part('year', start_time) desc) as year_order

    from
        historical_reservations
    where
        rn = 1
), 

final_result as (

select *, 

        "Capacity" - number_of_guest as "Unused seats",
        EXTRACT(EPOCH FROM (end_time - start_time)) / 60 AS "Meeting Duration",

        CASE 
            WHEN POSITION('cancelled' IN "details") = 0 THEN 'No show'
            WHEN "Capacity" IS NULL THEN NULL
            WHEN "number_of_guest" > "Capacity" THEN 'Over-used'
            WHEN "number_of_guest" < "Capacity" - 2 THEN 'Under-used'
            ELSE 'Right-sized'
        END AS "Booking Size",

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
        'MSGRAPH' as vendor  

from parsed_dates)

select
    *
from
    final_result