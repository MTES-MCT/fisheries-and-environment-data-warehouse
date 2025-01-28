CREATE TABLE monitorfish.fao_areas ENGINE MergeTree ORDER BY f_code AS 
SELECT
    * EXCEPT(wkb_geometry),
    readWKTMultiPolygon(wkb_geometry) AS wkb_geometry
FROM monitorfish.fao_areas_tmp
ORDER BY f_code