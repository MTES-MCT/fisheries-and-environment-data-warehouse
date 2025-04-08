DELETE FROM land_areas_subdivided;

INSERT INTO land_areas_subdivided (
    id, geometry
) VALUES
(1, ST_Polygon('LINESTRING(0.5 39.0, 4.5 39.0, 4.5 47.0, 0.5 47.0, 0.5 39.0)'::geometry, 4326)),
(2, ST_Polygon('LINESTRING(-6.5 41.0, -1.2 41.0, -1.2 53.0, -6.5 53.0, -6.5 41.0)'::geometry, 4326)),
(3, ST_Polygon('LINESTRING(1.0 30.0, 5.0 30.0, 5.0 35.0, 1.0 35.0, 1.0 30.0)'::geometry, 4326));
