{{
    config(materialized = 'materialized_view')
}}


WITH parsed_data AS (

            select
            timestamp at time zone 'America/Chicago' as timestamp,
            (
                jsonb_populate_recordset(null :: deliveries, payload :: jsonb)
            ).*
        from
            {{ source('raw_data_prod', 'audit') }}
        where
            subsystem = 'deliveriesPickedUp' 
            
            {% if is_incremental() %}

            and timestamp > max_time 
            
            {% endif %}

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

