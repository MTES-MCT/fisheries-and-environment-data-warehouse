WITH intersection_areas AS (
    SELECT
        fao.f_code,
        f.facade,
        SUM(ST_Area(ST_Intersection(f.geometry, fao.wkb_geometry)::geography) / 1e6) AS intersection_area_km2
    FROM fao_areas fao
    JOIN facade_areas_subdivided f
    ON ST_INTERSECTS(f.geometry, fao.wkb_geometry)
    GROUP BY fao.f_code, f.facade
    ORDER BY fao.f_code, f.facade
),

ranked_intersection_areas AS (
    SELECT
        f_code,
        facade,
        row_number() OVER (PARTITION BY f_code ORDER BY intersection_area_km2 DESC) AS rk
    FROM intersection_areas
),

fao_areas_facade AS (
    SELECT
        f_code,
        facade
    FROM ranked_intersection_areas
    WHERE rk=1
)

SELECT
    fao.*,
    COALESCE(f.facade, 'Hors façade') AS facade
FROM fao_areas fao
LEFT JOIN fao_areas_facade f
ON fao.f_code = f.f_code