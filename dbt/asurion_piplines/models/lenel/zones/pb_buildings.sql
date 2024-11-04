{{ config(materialized = 'ephemeral', tags = ['zones']) }} 



with zones as (
select zone_id, max(case when zone_type = 105 then right(zone_name,1) end) floor, 
max(case when zone_type = 103 or zone_type = 108 or zone_type = 102 then zone_name end) name_, 
max(case when zone_type = 101 then zone_name end) building

from zone
group by zone_id)

select * from zones 