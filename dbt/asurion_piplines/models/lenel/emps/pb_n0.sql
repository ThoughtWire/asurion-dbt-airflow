{{ config(materialized = 'ephemeral', tags=['emps']) }} 


with this_month as (
    select
         "EMPID" as empid, 
         "SVP_GROUP" as group_manager, 
         "ENTRPRS_HC" as entrprs_hc, 
         "RMID" as rmid, 
         "EMAIL" as email, 
         "EMAIL_CAP" as email_cap, 
         split_part("RMID",'  ',1) as bldgcode, 
         current_date as archdate, 
         to_char(current_date, 'YYYY-MM') as month_year,
        CASE 
          WHEN trim(split_part("RMID",'  ',1))='NGHB' THEN coalesce(trim(substring("RMID" from '[SNP]\d.*')), 'Unassigned')
          ELSE  coalesce(trim(split_part("RMID", '    ', 2)), 'Unassigned')
        END as "Room Name",

        coalesce(trim(split_part("RMID", '    ', 1)), 'Unassigned') as "Building Name",

        CASE 
          WHEN trim(split_part("RMID",'  ',1))='NGHB' THEN coalesce(substring(trim(substring("RMID" from '[SNP]\d.*')) from '[SNP]\d{1}(?=(\.|-))'), 'Unassigned')
          ELSE  split_part(trim(split_part("RMID", '    ', 2)), '.', 1)
        END as "Floor"

    from
        {{ source('raw_data_prod', 'dti_n0') }}
)

select * from this_month