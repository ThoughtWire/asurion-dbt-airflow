-- materiazlied view for insights entries which joins agg table with dto_headcount

{{ config(materialized = 'incremental', sort = '"EventDateTimeEastern"') }}

{% if is_incremental() %}
{% set max_time = get_max_value('insights_lenel_gulch', '"EventDateTimeEastern"') %}
{% endif %}


{% set bom = beginning_of_month('current_timestamp') %}


with updated_entries as (

select ssno, "devId" , "eventTime" ,  "employeeId" ,"eventDescription", 
"BusinessFunction" , "EntrprsAllEmployees", garage  

from {{ source('raw_data_prod', 'insights_entries_agg') }}

where bldgcode = 'NGHB' 

{% if is_incremental() %}

and "eventTime" > ({{ max_time }}) 

{% endif %}

),

time_zone as (


select ssno, "devId" , "eventTime" AT TIME ZONE 'America/Chicago' as "EventDateTimeEastern",
"employeeId" , "eventDescription", garage, "BusinessFunction" , "EntrprsAllEmployees"

from updated_entries

),

final_result as (


    select ssno, "devId",  "EventDateTimeEastern",
    "employeeId" as "EmpId", garage, "BusinessFunction", "EntrprsAllEmployees", da.*, 
    "EventDateTimeEastern" :: DATE as "EventDate",
    trim(to_char("EventDateTimeEastern", 'Day')) :: text as "WeekDay",
    trim(to_char("EventDateTimeEastern", 'Month')) :: text as "Month",
    date_part('dow', "EventDateTimeEastern") :: int as "Dayofweek#",
    date_part('year', "EventDateTimeEastern") :: int as "Year",
    date_part('month', "EventDateTimeEastern") :: int as "Month#",
    date_part('hour', "EventDateTimeEastern") :: int as "Hour",
    to_date(trim(to_char("EventDateTimeEastern", 'Mon yyyy')), 'Mon yyyy') as "MMYYEventDate"

from time_zone tz

left join {{ref('desk_assignment')}} da on (da.empid = tz.ssno) and 
(to_char(da.archdate , 'YYYY-MM') = to_char(tz."EventDateTimeEastern" , 'YYYY-MM')) 

)

select * from final_result

