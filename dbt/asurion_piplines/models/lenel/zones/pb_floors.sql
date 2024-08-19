{{ config(materialized = 'ephemeral', tags = ['zones']) }} 



with floors as (
    select
        *
    from
        zone
    where
        zone_type = 105
)
select
    *
from
    floors