CREATE TABLE monitorfish.eez_areas ENGINE MergeTree ORDER BY ogc_fid AS 
SELECT
    * EXCEPT(wkb_geometry),
    readWKTMultiPolygon(wkb_geometry) AS wkb_geometry
FROM monitorfish.eez_areas_tmp
ORDER BY ogc_fid