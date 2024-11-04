-- materiazlied view for insights entries which joins agg table with dto_headcount

{{ config(materialized = 'incremental', 
    unique_key = ['ssno', '"EmpId"', '"EventDateTimeEastern"'],
    incremental_strategy = 'delete+insert', 
    tags = ['lenel_global'],
    sort = '"EventDateTimeEastern"') }}

{% if is_incremental() %}
{% set max_time = get_max_value('insights_lenel_gulch', '"EventDateTimeEastern"') %}
{% endif %}


{% set bom = beginning_of_month('current_timestamp') %}


with updated_entries as (

select ssno, "devId" , "eventTime" ,  "employeeId" ,"eventDescription", 
"BusinessFunction" ,"BusinessFunctionAllEmployees", "EntrprsAllEmployees", garage , "bldgcode", "CountryName",  "CityName", 
"StateName", "Employee#"

from {{ source('raw_data_prod', 'insights_entries_agg') }}

{% if is_incremental() %}

where "eventTime" >=  date_trunc('day', {{ max_time }}::timestamp)

{% endif %}

),

time_zone as (


select  "eventTime" AT TIME ZONE  'UTC' AT TIME ZONE 'America/Chicago'  as "EventDateTimeEastern",
ssno, "devId" , "eventTime" ,  "employeeId" ,"eventDescription", 
"BusinessFunction" ,"BusinessFunctionAllEmployees", "EntrprsAllEmployees", garage , "bldgcode", "CountryName",  "CityName", 
"StateName", "Employee#"

from updated_entries

),

final_data as (

		select *, 
            dense_rank() over (order by "eventTime" :: DATE desc) as day_order,
            dense_rank() over (order by date_part('year', "eventTime") desc, date_part('month', "eventTime") desc) as month_order,
            dense_rank() over (order by date_part('year', "eventTime") desc) as year_order

            from time_zone

),

final_result as (


    select ssno, "devId",  "EventDateTimeEastern"::timestamp,
    "employeeId" as "EmpId", garage as "Garage", "BusinessFunction" ,"BusinessFunctionAllEmployees", "EntrprsAllEmployees",
     fd.bldgcode as "PropertyNameShort", "CountryName",  "CityName", "StateName", "Employee#", da."group_manager", 
     da.entrprs_hc, da.rmid, da.bldgcode, da.archdate, da.month_year, 
     coalesce(da."Room Name", 'Unassigned') as "Room Name", coalesce(da."Building Name",'Unassigned') as "Building Name", 
     coalesce(da."Floor",'Unassigned') as "Floor", coalesce(da."Floor#",'Unassigned') as "Floor#", 
    "EventDateTimeEastern" :: DATE as "EventDate",
    trim(to_char("EventDateTimeEastern", 'Day')) :: text as "WeekDay",
    trim(to_char("EventDateTimeEastern", 'Month')) :: text as "EventMonthName",
    date_part('dow', "EventDateTimeEastern") :: int as "Dayofweek#",
    date_part('year', "EventDateTimeEastern") :: int as "EventYear",
    date_part('month', "EventDateTimeEastern") :: int as "EventMonth",
    date_part('hour', "EventDateTimeEastern") :: int as "Hour",
    to_date(trim(to_char("EventDateTimeEastern", 'Mon yyyy')), 'Mon yyyy') as "MMYYEventDate", 

    'Week of ' ||  to_char(date_trunc('week', "EventDateTimeEastern"), 'Mon') || ' ' || 
    extract(day from date_trunc('week', "EventDateTimeEastern")) AS "WeekRange",
    to_char("EventDateTimeEastern", 'Day') || ', ' || to_char("EventDateTimeEastern", 'MM/DD/YYYY') AS "EventDate / DayofWeek",
    EXTRACT(WEEK FROM "EventDateTimeEastern") AS "WeekNumFirstDayOfMonth", 
  	CASE 
		WHEN "BusinessFunction" = 'APAC Exec' THEN 1
		WHEN "BusinessFunction" = 'Client Services' THEN 2
		WHEN "BusinessFunction" = 'Corp Development' THEN 3
		WHEN "BusinessFunction" = 'Customer Solutions' THEN 4
		WHEN "BusinessFunction" = 'Executive' THEN 5
		WHEN "BusinessFunction" = 'Finance' THEN 6
		WHEN "BusinessFunction" = 'HR' THEN 7
		WHEN "BusinessFunction" = 'Legal' THEN 8
		WHEN "BusinessFunction" = 'Marketing' THEN 9
		WHEN "BusinessFunction" = 'Product' THEN 10
		WHEN "BusinessFunction" = 'Program Management' THEN 11
		WHEN "BusinessFunction" = 'Security' THEN 12
		WHEN "BusinessFunction" = 'Supply Chain' THEN 13
		WHEN "BusinessFunction" = 'Technology' THEN 14
		WHEN "BusinessFunction" = 'UBIF' THEN 15
    ELSE 16
  	END AS "BusinessGroup #",

	CASE
        WHEN day_order <= 30  THEN 'Last 30 Days'
        Else 'NA' 
    END AS "Last 30 Days", 

    CASE
        WHEN day_order = 1  THEN 'Today'
        WHEN day_order = 2  THEN 'Yesterday'
    	Else 'NA' 
    END AS "Today",

    CASE
        WHEN year_order = 1  THEN 'YTD'
        Else 'NA' 
    END AS "YearLabel",

    CASE
        WHEN month_order = 1  THEN 'Current Month'
        WHEN month_order = 2  THEN 'Previous Month'
    	Else 'NA' 
    END AS "Month_label", 

	'Lenel' as "Vendor"

from final_data fd

left join {{ref('desk_assignment')}} da on (da.empid = fd.ssno) and 
(to_char(da.archdate , 'YYYY-MM') = to_char(fd."EventDateTimeEastern" , 'YYYY-MM')) 

)

select * from final_result

