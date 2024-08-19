{{ config(materialized = 'incremental') }}

{% if is_incremental() %}

{% set max_time = get_max_value('insights_chargepoint', 'timestamp') %}

{% endif %}

with evsession_data as (

select * from {{ source('raw_data_prod', 'audit') }}

where subsystem = 'EVChargingSessionsAudit'

{% if is_incremental() %}

and timestamp > ({{max_time}})

{% endif %}

),

raw_data as (


select (jsonb_populate_recordset(null::ChargePoint, payload::jsonb)).*, timestamp
from evsession_data

),


duplicates_removed as (

    select *, row_number() over (partition by "sessionID" order by timestamp desc) as rn
    from raw_data

),

parsed_data as (

    select "Energy", "endTime" at time zone 'UTC' at time zone 'America/Chicago' as "EndTimeCST", 
    "sessionID","stationID", "startTime" at time zone 'UTC' at time zone 'America/Chicago' as "StartTimeCST", 
    timestamp at time zone 'America/Chicago' as timestamp,
    "portNumber", 
    "userID" as "UserId", 
    extract(epoch from ("endTime" - "startTime"))/3600 as "Session duration",
    dense_rank() over (order by "startTime" :: DATE desc) as day_order,
    dense_rank() over (order by date_part('year', "startTime") desc, date_part('month', "startTime") desc) as month_order,
    dense_rank() over (order by date_part('year', "startTime") desc) as year_order
    
    from duplicates_removed
    where rn = 1
  
),


final_result as (

    select *,
    CONCAT("stationID", ', ', "portNumber")as "StationId Port",
    trim(to_char("StartTimeCST", 'Day')) :: text as "WeekDayname",
    trim(to_char("StartTimeCST", 'Month')) :: text as "StartTimeCSTMonthName",
    date_part('dow', "StartTimeCST") :: int as "StartTimeCSTDayOfWeek",
    "StartTimeCST" :: date as "StartTimeCSTDate",
    "EndTimeCST" :: date as "EndTimeCSTDate",
    date_part('year', "StartTimeCST") :: int as "StartTimeCSTYear",
    date_part('month', "StartTimeCST") :: int as "StartTimeCSTMonth",
    date_part('hour', "StartTimeCST") :: int as "StartTimeCSTHour",
    TO_CHAR("StartTimeCST", 'FMHH12 AM') as "Hour-AMPM",
    CASE 
        WHEN "Session duration" > 6 THEN 'Heavy [>6 hrs]'
        WHEN "Session duration" > 3 THEN 'Moderate [3-6 hrs]'
        ELSE 'Light [<3 hrs]'
        
    END as "Category",

    to_date(trim(to_char("StartTimeCST", 'Mon yyyy')), 'Mon yyyy') as  "MMYYEventDate",

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

    'Chargepoint' as vendor

    from parsed_data
)

select * from final_result