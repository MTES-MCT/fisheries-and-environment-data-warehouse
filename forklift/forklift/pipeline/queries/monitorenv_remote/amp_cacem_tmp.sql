SELECT
    *,
    ST_Area(geom::geography) / 1000000 AS surface_area_km2
FROM amp_cacem