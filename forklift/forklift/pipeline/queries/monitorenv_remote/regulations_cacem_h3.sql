SELECT
    id,
    ST_CollectionExtract(ST_MakeValid(ST_SimplifyPreserveTopology(geom, 0.0001))) AS geom,
    entity_name,
    layer_name,
    COALESCE(facade, 'NO_FACADE') AS facade,
    COALESCE(type, 'NO_TYPE') AS type
FROM regulations_cacem
WHERE 
  geom IS NOT NULL
  AND entity_name IS NOT NULL
  AND layer_name IS NOT NULL
  AND thematique IS NOT NULL
