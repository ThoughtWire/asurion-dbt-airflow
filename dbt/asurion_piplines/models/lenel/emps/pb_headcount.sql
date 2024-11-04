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

        CASE 
          WHEN bldgcode='NGHB' THEN coalesce(trim(substring(rmid from '[SNP]\d.*')), 'Unassigned')
          ELSE  coalesce(trim(split_part("rmid", '    ', 2)), 'Unassigned')
        END as "Room Name",

        coalesce(trim(split_part("rmid", '    ', 1)), 'Unassigned') as "Building Name",

        CASE 
          WHEN bldgcode='NGHB' THEN coalesce(substring(trim(substring(rmid from '[SNP]\d.*')) from '[SNP]\d{1}(?=(\.|-))'), 'Unassigned')
          ELSE  split_part(trim(split_part("rmid", '    ', 2)), '.', 1)
        END as "Floor"

    from
       {{ source('raw_data_prod', 'dti_headcount') }}

)

    select * from history