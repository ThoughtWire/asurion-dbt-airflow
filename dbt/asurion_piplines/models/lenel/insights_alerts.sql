{{ config(
    materialized = 'materialized_view',
    tags = ['alerts'], 
    indexes = [{'columns':['timestamp'], 'type' : 'btree'}]
) }} 


with parsed_data as (
    select
        source as subsystem,
        source as vendor,
        ac.message as category,
        value,
        eap."name" as priorityname,
        apm.message as prioritymessage,
        max(z."Building") as buildingname,
        max(z.room) as roomname,
        max(z."Floor") as floor,
        max(z."Floor#") as "Floor#",
        cah.start_time,
        cah.end_time
    from
        {{ source('raw_data_uat', 'cluster_alert_history') }} cah
        left join {{ source('raw_data_uat', 'alert_priority_message') }} apm on (apm.id = cah.priority_message_id)
        left join {{ source('raw_data_uat', 'alert_category') }} ac on (ac.id = apm.category_id)
        left join {{ source('raw_data_uat', 'enum_alert_priority') }} eap on (eap.id = apm.priority_id)
        left join {{ ref('pb_zones') }} z on (z.cluster_id = cah.cluster_id)
        and z.cluster_relationship_type = 200
    group by
        subsystem,
        vendor,
        category,
        value,
        priorityname,
        prioritymessage,
        start_time,
        end_time
),

parsed_dates as (
    select
        case
            when subsystem = '' then 'alerton'
            else subsystem
        end as subsystem,
        case
            when vendor = '' then 'alerton'
            else vendor
        end as vendor,
        category,
        value,
        COALESCE(priorityname, 'NA') as "priority_name",
        COALESCE(prioritymessage, 'NA') as "priority_message",
        COALESCE(buildingname, 'Asurion') as "buildingName",
        COALESCE(roomname, 'NA') as "roomName",
        COALESCE(floor, 'Asurion') as floor,
        "Floor#",
        start_time as timestamp,
        end_time as "endTime",
        start_time :: DATE as "timestamp_ymd",
        trim(to_char(start_time, 'Day')) :: text as "timestamp_weekday",
        trim(to_char(start_time, 'Month')) :: text as "timestampMonthName",
        date_part('dow', start_time) :: int as "timestampDayOfWeek",
        date_part('year', start_time) :: int as "timestamp_y",
        date_part('month', start_time) :: int as "timestampMonth",
        date_part('hour', start_time) :: int as "hour",
        trim(to_char(start_time, 'mm yyyy')) :: text as "MMYYEventDate",
        case
            when floor is NULL then 'Cloud'
            else 'Local'
        END as cloud_local,
        dense_rank() over (order by start_time :: DATE desc) as day_order,
        dense_rank() over (order by date_part('year', start_time) desc, date_part('month', start_time) desc) as month_order,

        CONCAT(
            case
                when subsystem = '' then 'alerton'
                else subsystem
            end,
            ' at ',
            to_char(start_time, 'DD Mon YYYY HH24:MI')
        ) as alert_id,
        CONCAT(
            case
                when subsystem = '' then 'alerton'
                else subsystem
            end,
            ' - ',
            case
                when floor is NULL then 'Cloud'
                else 'Local'
            END
        ) as system_cloud_local
    from
        parsed_data
),

final_result as (

select *, 

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
            WHEN month_order = 1 THEN 'Current Month'
            WHEN month_order = 2 THEN 'Previous Month'
        Else 'NA' 
        END AS "Month_label"    

from parsed_dates)



select
    *
from
    final_result