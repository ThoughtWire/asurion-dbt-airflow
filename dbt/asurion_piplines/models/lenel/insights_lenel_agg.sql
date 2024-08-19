-- materiazlied view for insights entries which joins agg table with dto_headcount

{{ config(materialized = 'ephemeral') }}


{% set max_time = get_max_value('insights_lenel', 'eventTime') %}
{% set bom = beginning_of_month('current_timestamp') %}


with updated_entries as (

select ssno, "devId" , "eventTime" ,  "employeeId" ,"eventDescription", timestamp  

from {{ source('raw_data_prod', 'insights_entries') }}

where "eventTime" > '2024-08-09'

{% if is_incremental() %}

where "eventTime" > ({{ max_time }}) and "eventTime" < (select current_date )

{% endif %}

),

duplicates_removed as (


select ssno, "devId" , "eventTime" ,  "employeeId" ,"eventDescription", max(timestamp) 
from updated_entries
group by ssno, "devId" , "eventTime" ,  "employeeId" ,"eventDescription"

),

final_result as (
select 
	ue.ssno, ue."devId" as "DevId", ue."eventTime" at time zone 'America/Chicago' as "eventDateTime",
	ue."employeeId" as "EmpId",
	zl."BLDGCODE" as "PropertyNameShort", 
	zl."GARAGE" as "Garage", 
	coalesce (ahi."CountryName",'Others') as "CountryName", 
	coalesce (ahi."CityName",'Others') as "CityName", 
	coalesce (ahi."StateName",'Others') as "StateName", 
	case when (b.group_manager is null and tog.group_manager is not null) then 'Asurion Visitors'
	when (b.group_manager is null and tog.group_manager is null) then 'Visitors (External)'
	else b.group_manager end as "BusinessFunction",
	coalesce (tog.group_manager,'Visitors (External)') as "BusinessFunctionAllEmployees",
	coalesce (tog.entrprs_hc,'Visitors (External)') as "EntrprsAllEmployees",
	--trim( split_part(tog.rmid,'    ',2) ), 
	c."Employee#",
	da."Floor", da."Floor#", da."Room Name", da.rmid as "RMID"



 	from duplicates_removed ue
 	
	left join {{ ref('pb_zl') }} zl on (zl."BADGEREAD" =ue."devId")
	left join asurion_hashed_info ahi on (ahi."PropertyNameShort" = zl."BLDGCODE")
	left join {{ ref('desk_assignment') }} da on (da.bldgcode = zl."BLDGCODE") and (da.empid =  ue.ssno) and (da.month_year = to_char(ue."eventTime" , 'YYYY-MM') )
	left join {{ ref('desk_assignment') }} tog on  (tog.empid =  ue.ssno) and ( tog.month_year= to_char(ue."eventTime" , 'YYYY-MM') )
	left join {{ ref('pb_total_emps') }} te on  (te."PropertyNameShort" =  zl."BLDGCODE") and (te."entrprs_hc" =  tog.entrprs_hc) and (te."BusinessFunction" =  tog.group_manager)
)

	select * from final_result;