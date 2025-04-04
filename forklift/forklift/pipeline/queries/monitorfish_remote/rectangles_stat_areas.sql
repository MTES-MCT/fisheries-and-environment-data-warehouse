WITH intersection_areas AS (
    SELECT
        r.icesname,
        f.facade,
        SUM(ST_Area(ST_Intersection(f.geometry, r.wkb_geometry)::geography) / 1e6) AS intersection_area_km2
    FROM rectangles_stat_areas r
    JOIN facade_areas_subdivided f
    ON ST_INTERSECTS(f.geometry, r.wkb_geometry)
    GROUP BY r.icesname, f.facade
    ORDER BY r.icesname, f.facade
),

ranked_intersection_areas AS (
    SELECT
        icesname,
        facade,
        row_number() OVER (PARTITION BY icesname ORDER BY intersection_area_km2 DESC) AS rk
    FROM intersection_areas
),

stat_rectangles_facade AS (
    SELECT icesname, facade
    FROM ranked_intersection_areas
    WHERE rk = 1
)

SELECT
    rectangles_stat_areas.*,
    COALESCE(stat_rectangles_facade.facade, 'Hors façade') AS facade
FROM rectangles_stat_areas
LEFT JOIN stat_rectangles_facade
ON rectangles_stat_areas.icesname = stat_rectangles_facade.icesname