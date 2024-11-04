{{ config(
    materialized = 'incremental',
    sort = 'timestamp',
    unique_key = '"registrationNumber"',
    incremental_strategy = 'delete+insert', 
    tags = ['splan']
) }} 


{% if is_incremental() %} 

{% set max_time = get_max_value('insights_splan', 'timestamp') %} 
    
{% endif %} 
    

with parsed_data as (
        select
            timestamp at time zone 'America/Chicago' as timestamp,
            (
                jsonb_populate_recordset(null :: splan, payload :: jsonb)
            ).*
        from
            {{ source('raw_data_prod', 'audit') }}
        where
            subsystem = 'visitorAudit' 
            
            {% if is_incremental() %}

            and timestamp > {{max_time}}::timestamp - interval '2 months'
            
            {% endif %}
    ),

    duplicates_removed as (
        select
            status,
            "hostName",
            "hostEmail",
            "visitType",
            "checkInDate",
            "checkOutDate",
            "meetingEndDate",
            "meetingStartDate",
            "registrationNumber",
            "timestamp",
            row_number() over (partition by "registrationNumber" order by timestamp desc) as visitor_rank

    from
            parsed_data

    ),

    parsed_time as (
        select
            status,
            "hostName",
            "hostEmail",
            "visitType" as "VisitType",
            "checkInDate",
            CASE
                WHEN "checkInDate" = '' THEN NULL
                ELSE TO_TIMESTAMP("checkInDate", 'MM-DD-YYYY"T"HH24:MI') :: timestamp
            END AS "CheckInDateTimeCST",
            CASE
                WHEN "checkOutDate" = '' THEN NULL
                ELSE TO_TIMESTAMP("checkOutDate", 'MM-DD-YYYY"T"HH24:MI') :: timestamp
            END AS "CheckOutDateTimeCST",
            CASE
                WHEN "meetingEndDate" = '' THEN NULL
                ELSE TO_TIMESTAMP("meetingEndDate", 'MM-DD-YYYY"T"HH24:MI') :: timestamp
            END AS "MeetingEndDateTimeCST",
            CASE
                WHEN "meetingStartDate" = '' THEN NULL
                ELSE TO_TIMESTAMP("meetingStartDate", 'MM-DD-YYYY"T"HH24:MI') :: timestamp
            END AS "MeetingStartDateTimeCST",
            "registrationNumber",
            timestamp
        from
            duplicates_removed
            where visitor_rank = 1 and status not in ('Registration is Cancelled', 'Temporary badge issued', 'Temporary badge returned')
    ),

    final_data as (

            select *,

            round(extract(epoch from ("MeetingEndDateTimeCST" - "MeetingStartDateTimeCST"))/3600) as "MeetingDuration",
            extract(epoch from ("CheckOutDateTimeCST" - "CheckInDateTimeCST"))/60 as "Onsite",
            round(extract(epoch from ("CheckOutDateTimeCST" - "MeetingEndDateTimeCST"))/3600) as "Duration of Stay after meeting ended",
            CASE 
                WHEN "CheckInDateTimeCST" is not Null THEN 
                    CASE
                        WHEN "CheckOutDateTimeCST" is not NULL THEN extract(epoch from ("CheckOutDateTimeCST" - "CheckInDateTimeCST"))/3600
                        ELSE extract(epoch from ("MeetingEndDateTimeCST" - "CheckInDateTimeCST"))/3600
                    END
                ELSE CASE 
                        WHEN "CheckOutDateTimeCST" is not null THEN extract(epoch from ("CheckOutDateTimeCST" - "MeetingStartDateTimeCST"))/3600
                        ELSE extract(epoch from ("MeetingEndDateTimeCST" - "MeetingStartDateTimeCST"))/3600
                    END
            END as  "Duration of stay", 

            CASE 

                WHEN "MeetingStartDateTimeCST" > Current_timestamp THEN 'EXPECTED'
                ELSE 'Historical'

            END as "DataLabel"
            
            from parsed_time

        ),


    partition_data as (

        select *, 

            dense_rank() over (partition by "DataLabel" order by "MeetingStartDateTimeCST" :: DATE desc) as day_order,
            dense_rank() over (partition by "DataLabel" order by date_part('year', "MeetingStartDateTimeCST") desc, date_part('month', "MeetingStartDateTimeCST") desc) as month_order,
            dense_rank() over (partition by "DataLabel" order by date_part('year', "MeetingStartDateTimeCST") desc) as year_order

            from final_data


    ),

    final_result as (
        select
            status,
            "hostName",
            "hostEmail",
            "CheckInDateTimeCST",
            "CheckOutDateTimeCST",
            "MeetingEndDateTimeCST",
            "MeetingStartDateTimeCST",
            "registrationNumber",
            "MeetingDuration",
            "Onsite", 
            "Duration of stay",
            "Duration of Stay after meeting ended", 
            "DataLabel",
            timestamp,
            timestamp :: DATE as "timestampDate",
            "MeetingEndDateTimeCST" :: DATE as "MeetingEndDateCST",
            "MeetingStartDateTimeCST" :: DATE as "MeetingStartDateCST",
            COALESCE("VisitType", 'Not Classified') as "VisitType",
            trim(to_char("MeetingStartDateTimeCST", 'Day')) :: text as "MeetingStartDateWeekDayName",
            trim(to_char("MeetingStartDateTimeCST", 'Month')) :: text as "MeetingStartMonthName",
            date_part('dow', "MeetingStartDateTimeCST") :: int as "MeetingStartDateWeekDay",
            date_part('year', "MeetingStartDateTimeCST") :: int as "meetingStartYearCST",
            date_part('month', "MeetingStartDateTimeCST") :: int as "MeetingStartDateMonth",
            date_part('hour', "MeetingStartDateTimeCST") :: int as "MeetingStartDateHour",
            COALESCE(pn."Room Name", pn_c."Room Name") as "Zone",
            COALESCE(pn."Room Name", pn_c."Building Name") as "Building Name",
            COALESCE(pn."Floor", pn_c."Floor") as "Floor",
            to_date(trim(to_char("MeetingStartDateTimeCST", 'Mon yyyy')), 'Mon yyyy') as "MMYYMeetingDate",
            COALESCE(
                pn."group_manager",
                pn_c."group_manager",
                'Not Classified'
            ) as "BusinessFunction",

            CASE 
                WHEN "status"='Checked-Out' THEN 1
                WHEN "status"='Checked-In' or "status" = 'Registered and Checked-in' THEN
                    CASE 
                        WHEN "CheckOutDateTimeCST" is Null THEN 0
                        ELSE 1
                    END
                ELSE 0
            END as "Checked Out", 

            CASE 

                WHEN "MeetingEndDateTimeCST" >= NOW() OR "DataLabel" = 'Expected' THEN 'Not Determined'
                WHEN "CheckOutDateTimeCST" IS NULL THEN 'Not Checked Out'
                WHEN "Duration of Stay after meeting ended" < 0 THEN 'Early Check-Out'
                WHEN "Duration of Stay after meeting ended" > 1 THEN 'Late Check-Out'
                ELSE 'On Time Check-Out'

            END AS "Check out delay",
            

            CASE
                WHEN day_order <= 30 and "DataLabel" = 'Historical' THEN 'Last 30 Days'
                Else 'NA' 
            END AS "Last 30 Days", 

            CASE
                WHEN day_order = 1 and "DataLabel" = 'Historical' THEN 'Today'
                WHEN day_order = 2 and "DataLabel" = 'Historical' THEN 'Yesterday'
            Else 'NA' 
            END AS "Today",

            CASE
                WHEN year_order = 1 and "DataLabel" = 'Historical' THEN 'YTD'
            Else 'NA' 
            END AS "YearLabel",

            CASE
                WHEN month_order = 1 and "DataLabel" = 'Historical' THEN 'Current Month'
                WHEN month_order = 2 and "DataLabel" = 'Historical' THEN 'Previous Month'
            Else 'NA' 
            END AS "Month_label", 


            'SPLAN' as vendor
        from
            partition_data pt

            left join {{ ref('pb_n0') }} pn on (pn.email = pt."hostEmail")
            left join {{ ref('pb_n0') }} pn_c on (pn_c.email_cap = pt."hostEmail")
    )

select
    *
from
    final_result