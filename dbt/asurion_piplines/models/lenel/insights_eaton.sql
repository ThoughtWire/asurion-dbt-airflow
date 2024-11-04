{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', 'id', 'cluster'],
    incremental_strategy = 'delete+insert', 
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
        cluster_type,
        subsystem,
        (timestamp AT TIME ZONE 'America/Chicago')::timestamp as timestamp,
        (timestamp AT TIME ZONE 'America/Chicago')::DATE as timestamp_date,
        date_part('dow', timestamp AT TIME ZONE 'America/Chicago')::int as dow,
        date_part('year', timestamp AT TIME ZONE 'America/Chicago')::int as yr,
        date_part('month', timestamp AT TIME ZONE 'America/Chicago')::int as mnth,
        date_part('hour', timestamp AT TIME ZONE 'America/Chicago')::int as hr,
        split_part(cluster, '/', 4) as device
    FROM {{source('raw_data_prod', 'metric')}}
    WHERE id IN ('Forward Energies 0.1 kWh', 'DemandForwardWatts', 'ForwardEnergy', 'SumEnergy')
    {% if is_incremental() %}
        AND timestamp > {{ max_time }}::timestamp - interval '2 days'
    {% endif %}
),

ranked_data AS (
    SELECT 
        *,
        CASE
            WHEN cluster_type = 'Eaton_MSBN' THEN 'North'
            WHEN cluster_type = 'Eaton_MSBS' THEN 'South'
            WHEN cluster_type = 'Eaton_MSBG' THEN 'Garage'
            ELSE 'Cafe'
        END as new_subsystem,
        dense_rank() OVER (ORDER BY timestamp_date DESC) as day_order
    FROM filtered_metric
)

SELECT
    rd.id,
    rd.subsystem,
    rd.cluster_type,
    rd.value,
    rd.cluster,
    rd.device as "Device",
    rd.timestamp as "timestamp",
    rd.timestamp_date as "TimestampDate",
    trim(to_char(rd.timestamp, 'Day'))::text as "WeekDay",
    trim(to_char(rd.timestamp, 'Month'))::text as "timestampMonthName",
    rd.dow as "timestampDayOfWeek",
    rd.yr as "timestampYear",
    rd.mnth as "timestampMonth",
    rd.hr as "timestampHour",
    to_date(trim(to_char(rd.timestamp, 'Mon yyyy')), 'Mon yyyy') as "MMYYEventDate",
    rd.new_subsystem as "New_subsystem",
    CASE
        WHEN rd.cluster_type = 'Eaton_MSBN' THEN 'North'
        WHEN rd.cluster_type = 'Eaton_MSBS' THEN 'South'
        WHEN rd.cluster_type = 'Eaton_MSBG' THEN 'Garage'
        WHEN rd.cluster_type = 'Eaton_MSB' THEN 'Cafe'
        ELSE pz."Building"
    END as "New_Building",
    'Historical' as "DataLabel",
    CASE 
        WHEN rd.day_order = 1 THEN 'Today'
        WHEN rd.day_order = 2 THEN 'Yesterday'
        ELSE 'NA'
    END AS "Today",
    pz."Building" as "Building",
    pz."Floor",
    pz."Floor#",
    pz.cluster_name,
    pz."Room Type",
    'Eaton' as "Vendor"
FROM ranked_data rd
LEFT JOIN {{ref('pb_zones')}} pz 
    ON pz.external_id = rd.cluster 
    AND pz.subsystem = 'eaton'
    AND pz.cluster_relationship_type = 201
    AND pz.bldgcode = 'NGHB'
