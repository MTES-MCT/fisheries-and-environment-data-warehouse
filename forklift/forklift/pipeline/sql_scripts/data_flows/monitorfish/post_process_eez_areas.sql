CREATE TABLE monitorfish.eez_areas ENGINE MergeTree ORDER BY iso_sov1 AS 
SELECT
    * EXCEPT(wkb_geometry),
    readWKTMultiPolygon(wkb_geometry) AS wkb_geometry
FROM monitorfish.eez_areas_tmp
ORDER BY iso_sov1