{{ config(materialized = 'materialized_view', tags= ["desks"]) }}


with desks as (

select * from {{ ref('pb_n0') }}
union all 
select * from {{ ref('pb_headcount') }}

),

final_result as (
    select
        *,
        case
            when "Building Name" = 'NGHB' then right("Floor", 1)
            else 'Unassigned'
        end "Floor#"
    from
        desks
)

select * from final_result