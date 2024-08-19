{{ config(
    materialized = 'materialized_view',
    indexes = [{'columns': ['timestamp'],
    'type': 'btree' }], tags = ['eaton']
) }} 


with filtered_metric as (
    
    select info::json->>'zone_name' as "subsystem", prediction as value, "timestamp"  from predictions
where subsystem ='SumEnergy'
), 


final_data as (

            select *, 

            dense_rank() over (order by timestamp :: DATE desc) as day_order,
            dense_rank() over (order by date_part('year', timestamp) desc, date_part('month', timestamp) desc) as month_order,
            dense_rank() over (order by date_part('year', timestamp) desc) as year_order
           
            from filtered_metric
),

final_result as (

        select subsystem, value,  timestamp,
        timestamp :: DATE as "TimestampDate",
        trim(to_char(timestamp, 'Day')) :: text as "WeekDay",
        trim(to_char(timestamp, 'Month')) :: text as "timestampMonthName",
        date_part('dow', timestamp) :: int as "timestampDayOfWeek",
        date_part('year', timestamp) :: int as "timestampYear",
        date_part('month', timestamp) :: int as "timestampMonth", 
        to_date(trim(to_char(timestamp, 'Mon yyyy')), 'Mon yyyy') as "MMYYEventDate",
        CASE
            WHEN subsystem = 'Eaton_MSBN' THEN 'North'
            WHEN subsystem = 'Eaton_MSBS' THEN 'South'
            WHEN subsystem = 'Eaton_MSBG' THEN 'Garage'
            ELSE 'Cafe'
        END as "New_subsystem", 
        
        CASE
            WHEN subsystem = 'Eaton_MSBN' THEN 'North'
            WHEN subsystem = 'Eaton_MSBS' THEN 'South'
            WHEN subsystem = 'Eaton_MSBG' THEN 'Garage'
            ELSE 'Cafe'
        END as "New_Building",
        
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


        'Predicted' as "DataLabel", 
        'SumEnergy' as id, 

        'Eaton' as "Vendor"

    FROM final_data

)


select * from final_result