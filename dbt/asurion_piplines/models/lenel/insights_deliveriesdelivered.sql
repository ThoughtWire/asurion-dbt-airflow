{{ config(
    materialized = 'materialized_view',
    tags = ['deliveries']
) }} 




WITH parsed_data AS (
    select
        timestamp at time zone 'America/Chicago' as timestamp,
        carrier, "lockerId", "deliveryId", "lockerSize", "timeOfPickUp",
        "timeOfDelivery", "trackingNumber1", "confirmationCode"
        from insights_delivered
),


duplicates_removed as (
    select
        *,
        row_number() over (
            partition by "deliveryId"
            order by
                timestamp desc
        ) as rn_num
    from
        parsed_data
),


delivery_data as (
    select
        "deliveryId" as "DeliveryID",
        "timeOfDelivery" as "TimeOfDelivery",
        "timeOfPickUp" as "TimeOfPickUpDeliveriesEndPoint",
        "lockerId" as "LockerID",
        "lockerSize" as "LockerSize"
    from
        duplicates_removed
    where
        rn_num = 1
        and "timeOfDelivery" is not null
),


merged_data as (
    select
        dd.*,
        CASE
            WHEN coalesce(
                dp."TimeOfPickUp",
                dd."TimeOfPickUpDeliveriesEndPoint"
            ) = 'null' THEN null
            ELSE coalesce(
                dp."TimeOfPickUp",
                dd."TimeOfPickUpDeliveriesEndPoint"
            ) :: timestamp
        END AS "TimeOfPickUpUTC",

        dense_rank() over (order by dd."TimeOfDelivery" :: DATE desc) as day_order,
        dense_rank() over (order by date_part('year', dd."TimeOfDelivery"::DATE) desc, 
        date_part('month', dd."TimeOfDelivery"::DATE) desc) as month_order,
        dense_rank() over (order by date_part('year', dd."TimeOfDelivery"::DATE) desc) as year_order
    from
        delivery_data dd
        left join {{ ref('insights_deliveriespickedup') }} dp on (dp."DeliveryID" = dd."DeliveryID")
        and (dp."LockerID" = dp."LockerID")
),


final_result as (

    select
        
        "TimeOfDelivery" as "timestamp",

        NULL::int as "Available", 
        NULL::int as "Occupied", 
        NULL::int as "Reserved", 
        NULL::int as "Out_Of_Service",
        "LockerSize" as "Locker Size",
        "DeliveryID",
        "TimeOfPickUpUTC",
        EXTRACT(day from "TimeOfPickUpUTC" - "TimeOfDelivery")::int "Pick Up Duration",
        CASE 
            WHEN EXTRACT(day from "TimeOfPickUpUTC" - "TimeOfDelivery") < 3 THEN 'Less Than 3 Days'
            WHEN EXTRACT(day from "TimeOfPickUpUTC" - "TimeOfDelivery") < 7 THEN 'Less Than a Week'
            ELSE 'More Than a Week'
        END as "Pick up Duration Comparison",
        NULL as "Type",
        EXTRACT(day from current_date - "TimeOfDelivery")::int "Held in Lockers",
        coalesce("TimeOfPickUpUTC" is not null::int, 0)::int "picked up", 
        CASE 
            WHEN "LockerSize"='Small' THEN 1
            WHEN "LockerSize"='Medium' THEN 2
            ELSE 3
        END as "LockerType#",
        "TimeOfDelivery" :: DATE as "TimestampDate",
        trim(to_char("TimeOfDelivery", 'Day')) :: text as "Day",
        trim(to_char("TimeOfDelivery", 'Month')) :: text as "Month",
        date_part('dow', "TimeOfDelivery") :: int as "Dayofweek#",
        date_part('year', "TimeOfDelivery") :: int as "Year",
        date_part('month', "TimeOfDelivery") :: int as "Month#",
        date_part('hour', "TimeOfDelivery") :: int as "Hour",
        to_date(trim(to_char("TimeOfDelivery", 'Mon yyyy')), 'Mon yyyy') as "MMYYEventDate",


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
        'Deliveries' as vendor

    from
        merged_data
)


select
    *
from
    final_result