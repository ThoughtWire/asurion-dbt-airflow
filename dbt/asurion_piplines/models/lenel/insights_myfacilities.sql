{{ config(
    materialized = 'materialized_view',
    tags = ['myfacilities']
) }} 

WITH parsed_data AS (

            select
            timestamp at time zone 'America/Chicago' as timestamp,
            (
                jsonb_populate_recordset(null :: servicetickets, payload :: jsonb)
            ).*
        from

            {{ source('raw_data_prod', 'audit') }}

            
            where subsystem = 'workOrderInProgressServiceAudit' or 
            subsystem = 'workOrderHistoricalServiceAudit'
            
            ),
            
grouped_data as (

	select *, row_number() over (partition by "REQNO", "REQFORID", "ROOMFOR" order by timestamp desc ) as row_num
								
	from parsed_data
	where "PROBLEM" ~ ('(.*test test test.*) | (.*disregard.*) | (.*ignore.*) | (.*#test#.*)') or 
	"ACTION" !~ ('(.*test test test.*) | (.*disregard.*) | (.*ignore.*) | (.*#test#.*)')
	and ("CATEGORY" != 'AV          ')

),

final_result as (   

select "REQNO", "ACTION", "PROBLEM", "ACTIVITY", "BLDGCODE", 
		"CATEGORY", "PRIORITY", "REQFORID",                      
   CASE 

    WHEN "STATUS" IN ('P', 'Reopen', 'A', 'F', 'O') THEN 'Open'
    WHEN "STATUS" IN ('R', 'C', 'Q') THEN 'Closed'
    WHEN "STATUS" = 'H' THEN 'On-Hold'
    WHEN "STATUS" = 'X' THEN 'Cancelled'
    WHEN "STATUS" = 'Z' THEN 'Dispatched'
    ELSE CONCAT('Others - ', "STATUS")

END AS "STATUS",

    NULLIF("DUEDATE", 'null')::timestamp as "DUEDATE", "ROOMFOR", NULLIF("DATERCVD", 'null')::timestamp as "DATERCVD", 
    NULLIF("COMPLDATE", 'null')::timestamp as "COMPLDATE", timestamp, 'MyFacilities' as vendor

    from grouped_data

    where row_num=1 and "ROOMFOR" like '%NGHB%')

select * from final_result