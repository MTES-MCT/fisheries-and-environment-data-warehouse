CREATE TABLE monitorfish.non_overlapping_fao_areas ENGINE MergeTree ORDER BY f_code AS 
SELECT
    * EXCEPT(geometry),
    readWKTMultiPolygon(geometry) AS geometry
FROM monitorfish.non_overlapping_fao_areas_tmp
ORDER BY f_code