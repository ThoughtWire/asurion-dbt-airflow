{{ config(
    materialized = 'materialized_view',
    tags = ['myfacilities']
) }} 



With workorders as (

    select *,
    round(extract(epoch from ("COMPLDATE" - "DATERCVD"))/3600) as "DurationHours",
    trim(split_part("ROOMFOR", ' ', 1)) "PortfolioBuilding", 
    CASE
        WHEN trim((regexp_match("ROOMFOR", '(?<=\s).*'))[1])='' THEN 'Other'
        ELSE trim((regexp_match("ROOMFOR", '(?<=\s).*'))[1])
    END AS "ZoneName",
    "DATERCVD"::DATE "DATERCVDD",
    "DUEDATE"::DATE "DUEDATED",
    "COMPLDATE"::DATE "COMPLDATED",

    CASE 
        WHEN "STATUS" = 'Closed' THEN 
            CASE 
                WHEN "COMPLDATE" IS NULL THEN "DATERCVD"::DATE
                 ELSE "COMPLDATE"::DATE 
            END
        ELSE "DATERCVD"::DATE
    END AS "TicketDate"

     from {{ref('insights_myfacilities')}}
),


time_zone as (

    Select *, 
    CASE 
        WHEN SUBSTRING("ZoneName" FROM 2 FOR 1) not in ('1','2','3','4','5','6','7','8','9') THEN 'Other'
        ELSE SUBSTRING("ZoneName" FROM 2 FOR 1)
    END AS "Floor#",

    CASE
        WHEN LEFT(TRIM(SPLIT_PART("ZoneName", '.', 1)), 2) !~ '(N|S|P)\d' THEN 'Other'
        ELSE TRIM(SPLIT_PART("ZoneName", '.', 1))
    END AS "Floor",

    CASE 
        WHEN LEFT("ZoneName", 2) ~ 'N\d' THEN 'North'
        WHEN LEFT("ZoneName", 2) ~ 'S\d' THEN 'South'
        WHEN LEFT("ZoneName", 2) ~ 'P\d' THEN 'Parking'
        ELSE 'NGHB'
    END AS "NGHBBuilding",
    trim(to_char("DATERCVD", 'Day')) :: text as "requestDayOfWeekName",
    trim(to_char("DATERCVD", 'Month')) :: text as "requestMonthName",
    date_part('dow', "DATERCVD") :: int as "requestDayofWeek",
    date_part('year', "DATERCVD") :: int as "RequestYear",
    date_part('month', "DATERCVD") :: int as "requestMonth",

    dense_rank() over (order by "DATERCVD" :: DATE desc) as day_order,
    dense_rank() over (order by date_part('year', "DATERCVD") desc, date_part('month', "DATERCVD") desc) as month_order,
    dense_rank() over (order by date_part('year', "DATERCVD") desc) as year_order,
    EXTRACT(DAY FROM ("COMPLDATE"  - "DATERCVD")) "ResolutionDays",
    EXTRACT(DAY FROM ("DUEDATE"   - Current_date)) "ToDueDate",

    EXTRACT(DAY FROM (Current_date - "DUEDATE")) AS "PastDueDate"

    from workorders

),

final_result as (

    select *, 

        CASE
            WHEN day_order <= 30 THEN 'Last 30 Days'
            Else 'NA' 
        END AS "Last 30 Days", 

        CASE
            WHEN day_order = 1 THEN 'Today'
            WHEN day_order = 2 THEN 'Yesterday'
            Else 'NA' 
        END AS "Today",

        CASE
            WHEN year_order = 1 THEN 'YTD'
            Else 'NA' 
        END AS "YearLabel",

        CASE
            WHEN month_order = 1 THEN 'Current Month'
            WHEN month_order = 2 THEN 'Previous Month'
            Else 'NA' 
        END AS "Month_label", 

        to_date(trim(to_char("DATERCVD", 'Mon yyyy')), 'Mon yyyy') as "MMYYEventDate",

        CASE 
            WHEN "DUEDATE" > Current_date and "STATUS" != 'Closed' THEN 1
            ELSE 0
        END AS "Upcoming Tickets",

        CASE 
            WHEN "DUEDATE" < Current_date and "STATUS" != 'Closed' THEN 1
            ELSE 0
        END AS "Overdue Tickets"

    from time_zone

)


select * from final_result