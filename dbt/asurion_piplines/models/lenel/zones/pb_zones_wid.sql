{{ config(materialized = 'ephemeral', 
tags=['zones']) }}


with zones as (
    select
        c.cluster_id,
        c.external_id,
        c.cluster_name,
        c.subsystem,
        cr.cluster_relationship_type,
        cr.zone_id,
        max(f.zone_name) as floor,
        max(r.zone_name) as room,
        max(b.zone_name) as building,
        max(u.zone_name) as unit
    from
        cluster c
        left join cluster_relationship cr on (cr.cluster_id = c.cluster_id)
        left join {{ ref('pb_floors') }} f on (f.zone_id = cr.zone_id) 
        left join {{ ref('pb_rooms') }} r on (r.zone_id = cr.zone_id)
        left join {{ ref('pb_buildings') }} b on (b.zone_id = cr.zone_id)
        left join {{ ref('pb_units') }} u on (u.zone_id = cr.zone_id)

    group by

        c.cluster_id,
        c.external_id,
        c.cluster_name,
        c.subsystem,
        cr.cluster_relationship_type,
        cr.zone_id
),

final_zones as (
    select
        cluster_id,
        external_id,
        cluster_name,
        subsystem,
        cluster_relationship_type,
        zone_id,
        CASE
            WHEN floor is NULL THEN 
                CASE 
                    WHEN subsystem like '%Alerton%' THEN (regexp_match(room, '(?<=(N|S|P))\d(?=\.)'))[1]
                    ELSE (regexp_match(cluster_name, '(?<=NGHB [NS])\d(?=\.)'))[1]
                END
            ELSE floor
        END as "Floor#",
        CASE
            WHEN building IS NULL THEN 
                CASE
                    WHEN subsystem like '%Alerton%' THEN 
                        CASE
                            WHEN (regexp_match(room, '.'))[1] = 'N' THEN 'North'
                            WHEN (regexp_match(room, '.'))[1] = 'P' THEN 'Parking'
                            WHEN (regexp_match(room, '.'))[1] = 'S' THEN 'South'
                            ELSE 'Asurion'
                        END
                    WHEN (regexp_match(cluster_name, '(?<=NGHB ).(?=\d)'))[1] = 'N' THEN 'North'
                    ELSE 'South'
                END
            ELSE building
        END AS "Building",
        CASE
            WHEN room is null THEN trim((regexp_match(cluster_name, '(?<=NGHB ).+'))[1])
            ELSE room
        END as room,
        unit
    from
        zones z
),


final_result as (
    select
        fz.*,
        Concat(left("Building", 1), right("Floor#", 1)) as "Floor",
        da.svp_occpln,
        da.space_type as "Room Type",
        da.bldgcode,
        da.capacity
    from
        final_zones fz
        left join {{ source('raw_data_prod', 'dti_a0') }} da on (da.rmid = fz.room)
)

select
    *
from
    final_result