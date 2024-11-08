{{ config(materialized = 'materialized_view', 
tags=['zones']) }}


with zones as (
    select
        c.cluster_id,
        c.external_id,
        c.cluster_name,
        c.subsystem,
        c.cluster_type,
        cr.cluster_relationship_type,
        cr.zone_id,
        b.floor as floor,
        b.name_ as room,
        b.building as building
        --max(u.zone_name) as unit
    from
        cluster c
        left join cluster_relationship cr on (cr.cluster_id = c.cluster_id)
        left join {{ ref('pb_buildings') }} b on (b.zone_id = cr.zone_id)
),

final_zones as (
    select
        cluster_id,
        external_id,
        cluster_name,
        cluster_type,
        subsystem,
        zone_id,
        cluster_relationship_type,
        CASE
            WHEN floor is NULL THEN (regexp_match(room, '(?<=[NS])\d(?=\.|$)'))[1]
            ELSE floor
        END as "Floor#",
        CASE
            WHEN building IS NULL THEN 
                        CASE
                            WHEN (regexp_match(room, '.'))[1] = 'N' THEN 'North'
                            WHEN (regexp_match(room, '.'))[1] = 'P' THEN 'Parking'
                            WHEN (regexp_match(room, '.'))[1] = 'S' THEN 'South'
                            ELSE 'Asurion'
                        END
            ELSE building
        END AS "Building",
        CASE
            WHEN room is null THEN 'Asurion'
            ELSE room
        END as room
        --unit
    from
        zones z
),


final_result as (
    select
        fz.*,
        Concat(left("Building", 1), right("Floor#", 1)) as "Floor",
        pc.svp_occpln,
        pc.type_ as "Room Type",
        pc.bldgcode,
        pc.capacity_ as capacity
    from
        final_zones fz
        left join {{ref('pb_capacity')}} pc on (pc.zone_id = fz.zone_id)
)

select
    *
from
    final_result