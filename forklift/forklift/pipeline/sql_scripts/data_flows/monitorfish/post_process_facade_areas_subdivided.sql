CREATE TABLE monitorfish.facade_areas_subdivided ENGINE MergeTree ORDER BY facade AS 
SELECT
    * EXCEPT(geometry),
    readWKTPolygon(geometry) AS geometry
FROM monitorfish.facade_areas_subdivided_tmp
ORDER BY facade