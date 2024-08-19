{{ config(materialized = 'ephemeral', tags = ['emps']) }} 



with total_emps as (

select group_manager as "BusinessFunction", entrprs_hc, "Building Name" as "PropertyNameShort", count(*) as "Employee#" 
	from {{ ref('pb_n0') }} dn
	group by group_manager, entrprs_hc, "Building Name"

)


select * from total_emps