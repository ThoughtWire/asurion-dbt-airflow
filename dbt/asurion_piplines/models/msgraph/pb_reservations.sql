{{ config(materialized = 'materialized_view') }} 

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
        to_char(timestamp, 'Mon YYYY') as timestamp,
        count(*) as cnt
    from
        release
    group by
        to_char(timestamp, 'Mon YYYY')
),

final_result as (
select
    r.id,
    array_length(guests_id, 1) as number_of_guest,
    z.zone_name,
    da.capacity as Capacity,
    da.type_name as Type_Name,
    r.online_meeting_provider,
    r.subject,
    r.start_time,
    r.end_time,
    r.status,
    eb.details,
    ea.timestamp,
    ea.cnt
from
    reservation_history r
    left join zone z on (z.zone_id = r.zone_id)
    left join enum_booking_action eb on (eb.id = r.status)
    left join insights_ioffice_roominfo da on (da.name = z.zone_name)
    left join emails ea on (ea.timestamp = to_char(r.start_time, 'Mon YYYY'))
where
    r.zone_id is not null
    and subject not ilike '%test%'
    and r.start_time > '2023-01-01')

select * from final_result