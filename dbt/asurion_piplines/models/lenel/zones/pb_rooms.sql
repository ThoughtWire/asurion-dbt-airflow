{{ config(materialized = 'ephemeral', tags = ['zones']) }} 



with rooms as (
    select
        *
    from
        zone
    where
        zone_type in (103, 108)
)
select
    *
from
    rooms