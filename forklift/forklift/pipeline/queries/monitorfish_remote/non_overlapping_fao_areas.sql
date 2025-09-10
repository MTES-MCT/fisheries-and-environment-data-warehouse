SELECT
    f.f_code,
    COALESCE(
        ST_Difference(
            f.wkb_geometry,
            ST_Union(ST_Intersection(f.wkb_geometry, f2.wkb_geometry))
        ),
        f.wkb_geometry
    ) AS geometry
FROM fao_areas f
LEFT JOIN fao_areas f2
ON  
    ST_Intersects(f.wkb_geometry, f2.wkb_geometry) AND
    f2.f_code != f.f_code AND
    ST_Area(ST_Intersection(f.wkb_geometry, f2.wkb_geometry)::geography) >= 0.99 * ST_Area(f2.wkb_geometry::geography)
GROUP BY 1
HAVING COALESCE(ST_Area(ST_Union(ST_Intersection(f.wkb_geometry, f2.wkb_geometry))::geography) / ST_Area(f.wkb_geometry::geography), 0) < 0.99