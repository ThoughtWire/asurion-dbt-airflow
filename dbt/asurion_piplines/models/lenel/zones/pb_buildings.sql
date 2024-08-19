{{ config(materialized = 'ephemeral', tags = ['zones']) }} 



with buildings as (
    select
        zone_id, 
        case 
            when zone_name = 'North Building' then 'North'
            when zone_name = 'South Building' then 'South'
            else zone_name
        end as zone_name, zone_position, zone_type
    from
        zone
    where
        zone_type = 109
)
select
    *
from
    buildings