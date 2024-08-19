{{ config(
    materialized = 'incremental',
    indexes = [{'columns': ['timestamp'],
    'type': 'btree' }], tags = ['eaton']
) }} 


 {% if is_incremental() %}

{% set max_time = get_max_value('insights_eaton', 'timestamp') %}

{% endif %}


WITH filtered_metric AS (
    SELECT
        id,
        cluster,
        value,
        (timestamp at time zone 'America/Chicago') as "timestamp" 
    FROM
        {{source('raw_data_prod', 'metric')}}
    where
        id = ANY (ARRAY['Forward Energies 0.1 kWh', 'DemandForwardWatts', 'ForwardEnergy', 
        'SumEnergy'])

        {% if is_incremental() %}

           and (timestamp at time zone 'America/Chicago') > {{ max_time }}

        {% endif %}

    order by

        "timestamp" desc

), 

final_data as (

            select *, 

            dense_rank() over (order by timestamp :: DATE desc) as day_order,
            dense_rank() over (order by date_part('year', timestamp) desc, date_part('month', timestamp) desc) as month_order,
            dense_rank() over (order by date_part('year', timestamp) desc) as year_order
           
            from filtered_metric
),

final_result as (

    select
        fd.id,
        subsystem,
        value,
        cluster,
        split_part(cluster, '/', 4) as "Device",
        "timestamp",
        timestamp :: DATE as "TimestampDate",
        trim(to_char(timestamp, 'Day')) :: text as "WeekDay",
        trim(to_char(timestamp, 'Month')) :: text as "timestampMonthName",
        date_part('dow', timestamp) :: int as "timestampDayOfWeek",
        date_part('year', timestamp) :: int as "timestampYear",
        date_part('month', timestamp) :: int as "timestampMonth",
        date_part('hour', timestamp) :: int as "timestampHour",
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
            WHEN subsystem = 'Eaton_MSB' THEN 'Cafe'
            ELSE pz."Building"
        END as "New_Building",

        'Historical' as "DataLabel",

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

        pz."Building" as "Building", pz."Floor", pz."Floor#", pz.cluster_name, pz."Room Type", 
        'Eaton' as "Vendor"
    from
        final_data fd
        
        left join {{ref('pb_zones')}} pz on (pz.external_id = fd."cluster")
)

select
    *
from
    final_result