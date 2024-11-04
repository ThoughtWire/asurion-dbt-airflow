{{ config(materialized = 'materialized_view', tags= ["desks"]) }}


with desks as (

select empid, group_manager, entrprs_hc, rmid, bldgcode, archdate, month_year, 
"Room Name", "Building Name", "Floor" from {{ ref('pb_n0') }}
union all 
select * from {{ ref('pb_headcount') }}

),

final_result as (
    select
        *,
        case
            when "Building Name" = 'NGHB' and Left("Floor", 1) in ('S', 'N', 'P') then coalesce(right("Floor", 1), 'Unassigned')
            else 'Unassigned'
        end "Floor#"
    from
        desks
)

select * from final_result