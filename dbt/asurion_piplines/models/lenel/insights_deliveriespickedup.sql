{{
    config(materialized = 'materialized_view', 
    tags = ['deliveries'])
}}


WITH parsed_data AS (

    select
        timestamp at time zone 'America/Chicago' as timestamp,
        carrier, "lockerId", "deliveryId", "lockerSize", "timeOfPickUp",
        "timeOfDelivery", "trackingNumber1", "confirmationCode"
        from {{source('raw_data_prod', 'insights_pickedup')}}

            ), 

duplicates_removed as (

           select *, row_number() over (partition by "deliveryId" order by timestamp desc) as rn_num
           from parsed_data
),

final_result as (

    select timestamp, "deliveryId" as "DeliveryID", "timeOfDelivery" as "TimeOfDelivery", "timeOfPickUp" as "TimeOfPickUp", 
    "lockerId" as "LockerID", "lockerSize" as "LockerSize"

    from duplicates_removed
    where rn_num=1

)

select * from final_result

