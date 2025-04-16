CREATE TABLE monitorenv.regulations_cacem ENGINE MergeTree ORDER BY id AS 
SELECT
    * EXCEPT(geom),
    readWKTMultiPolygon(geom) AS geom
FROM monitorenv.regulations_cacem_tmp
ORDER BY id