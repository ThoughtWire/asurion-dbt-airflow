{{ config(materialized = 'ephemeral', tags = ['zones']) }} 



with units as (
    select
        *
    from
        zone
    where
        zone_type = 102
)
select
    *
from
    units