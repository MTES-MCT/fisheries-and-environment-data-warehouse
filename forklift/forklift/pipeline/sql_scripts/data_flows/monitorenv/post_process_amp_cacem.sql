CREATE TABLE monitorenv.amp_cacem ENGINE MergeTree ORDER BY id AS 
SELECT
    * EXCEPT(geom),
    readWKTMultiPolygon(geom) AS geom
FROM monitorenv.amp_cacem_tmp
ORDER BY id