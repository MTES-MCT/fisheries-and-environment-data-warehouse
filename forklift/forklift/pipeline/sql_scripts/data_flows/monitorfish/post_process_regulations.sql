CREATE TABLE monitorfish.regulations ENGINE MergeTree ORDER BY id AS 
SELECT
    * EXCEPT(geometry_simplified),
    readWKTMultiPolygon(geometry_simplified) AS geometry_simplified
FROM monitorfish.regulations_tmp
ORDER BY id