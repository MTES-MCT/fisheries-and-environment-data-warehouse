WITH ports_nearby_position_at_sea AS (
    SELECT
        locode,
        ST_Centroid(
            ST_Difference(
                ST_Buffer(ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326), 0.2),
                    ST_Union(
                    ST_Intersection(
                        ST_Buffer(ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326), 0.2),
                        St_MakeValid(l.geometry)
                    )
                )
            )
        ) AS nearby_position_at_sea
    FROM ports p
    JOIN land_areas_subdivided l
    ON
        ST_Intersects(
            ST_Buffer(ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326), 0.2),
            l.geometry
        )
    WHERE
        facade IS NOT NULL OR
        is_active
    GROUP BY 1
)

SELECT
    p.locode,
    country_code_iso2,
    region,
    port_name,
    latitude,
    longitude,
    COALESCE(facade, 'Hors façade') AS facade,
    fao_areas,
    COALESCE(is_active, false) AS is_active,
    ST_X(nearby_position_at_sea) AS nearby_position_at_sea_longitude,
    ST_Y(nearby_position_at_sea) AS nearby_position_at_sea_latitude
FROM ports p
LEFT JOIN ports_nearby_position_at_sea pnoas
ON p.locode = pnoas.locode