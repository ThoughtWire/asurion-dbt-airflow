{{ config(materialized = 'materialized_view', tags=['emps']) }} 


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
        trim(split_part("RMID", '    ', 2)) as "Room Name",
        trim(split_part("RMID", '    ', 1)) as "Building Name",
        split_part(trim(split_part("RMID", '    ', 2)), '.', 1) as "Floor"
    from
        {{ source('raw_data_prod', 'dti_n0') }}
)

select * from this_month