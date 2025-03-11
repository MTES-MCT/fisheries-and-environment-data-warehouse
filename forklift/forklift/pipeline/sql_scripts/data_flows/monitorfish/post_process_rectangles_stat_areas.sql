CREATE TABLE monitorfish.rectangles_stat_areas ENGINE MergeTree ORDER BY id AS 
SELECT
    * EXCEPT(wkb_geometry),
    readWKTMultiPolygon(wkb_geometry) AS wkb_geometry
FROM monitorfish.rectangles_stat_areas_tmp
ORDER BY id