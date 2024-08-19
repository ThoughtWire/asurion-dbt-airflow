{{ config(materialized = 'ephemeral', tags=['emps']) }} 


with history as (
  select
        empid , 
        group_manager,
        entrprs_hc, 
        rmid, 
        bldgcode, 
        archdate ,
        to_char(archdate, 'YYYY-MM') as month_year,
        trim(split_part("rmid", '    ', 2)) as "Room Name",
        trim(split_part("rmid", '    ', 1)) as "Building Name",
        split_part(trim(split_part("rmid", '    ', 2)), '.', 1) as "Floor"
    from
       {{ source('raw_data_prod', 'dti_headcount') }}

)

    select * from history