{{config(
    materialized ='ephemeral', 
    unique_key = ['zone_id', 'bldgcode']

)}}


with zone_property as (
select zone_id, max(case when zone_property_type = 20 then property_value end) as type_,
max(case when zone_property_type = 21 then property_value end) as name_,
max(case when zone_property_type = 11 then property_value end) as capacity_

from zone_property z
WHERE zone_property_type IN (11, 20, 21)
group by z.zone_id),

zones as (

select z.zone_id, max(case when zone_type = 105 then zone_name end) as floor_,
max(case when zone_type = 108 or zone_type = 103 or zone_type = 102 then zone_name end) as name_,
max(case when zone_type = 101 then zone_name end) as building_

from zone z
WHERE zone_type IN (103, 105, 108, 101, 102)
group by z.zone_id),

final_zones as (
select z.zone_id, z.name_,  zp.type_, zp.capacity_ from zones z 
left join zone_property zp on (z.zone_id = zp.zone_id))

select zone_id, name_ as room, 
case
	when fz.capacity_ is null then da.capacity::int 
	else fz.capacity_::int
	
end as capacity_, 
case
	when fz.type_ is null then da.space_type 
	else fz.type_
	
end as type_, 

svp_occpln, bldgcode
from final_zones fz
left join dti_a0 da on (da.rmid = fz.name_)