{{ config(materialized = 'incremental') }}

{% if is_incremental() %}
  {% set max_time = get_max_value('insights_chargepoint_status', '"timestampCST"') %}
{% endif %}

WITH statuses AS (
    SELECT
        *
    FROM
        {{ source('raw_data_prod', 'audit') }}
    WHERE
        subsystem = 'EVChargingStatusAudit'

        {% if is_incremental() %}

        AND "timestamp" > {{ max_time }}

        {% endif %}
),

stations AS (
    SELECT
        timestamp at time zone 'America/Chicago' AS timestamp,
        items."ports",
        items."stationID"
    FROM
        statuses,
        jsonb_to_recordset(statuses.payload) AS items("ports" jsonb, "stationID" text)
    WHERE
        items."stationID" != '1:11979541'
),

ports_ AS (

    SELECT
        timestamp,
        "stationID",
   		 port->>'Status' AS "Status",
   		 port->>'portNumber' AS "portNumber"
    FROM
        stations,
        jsonb_array_elements(ports) AS port  -- Check if this function and type cast are correct
),

final_result AS (
    SELECT
        "Status",
        timestamp as "timestampCST",
        COUNT(*) AS NumberofPorts,
        timestamp :: DATE as "timestampDate",
        trim(to_char(timestamp, 'Day')) :: text as "WeekDayName",
        trim(to_char(timestamp, 'Month')) :: text as "timestampMonthName",
        date_part('dow', timestamp) :: int as "WeekDay",
        date_part('year', timestamp) :: int as "timestampYear",
        date_part('month', timestamp) :: int as "timestampMonth",
        date_part('hour', timestamp) :: int as "timestampHour",
        TO_CHAR(timestamp, 'FMHH12 AM') as "Hour-AMPM"

    FROM
        ports_
   GROUP BY
       timestamp, "Status"
    
)

SELECT
    *
FROM
    final_result

where "WeekDayName" not in ('Sunday', 'Saturday')
