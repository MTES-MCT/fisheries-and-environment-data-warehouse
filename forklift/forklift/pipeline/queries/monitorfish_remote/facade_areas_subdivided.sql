SELECT
    facade,
    ST_Multi(geometry) AS geometry,
    id
FROM facade_areas_subdivided