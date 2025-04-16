SELECT
    id,
    geom,
    entity_name,
    url,
    layer_name,
    facade,
    ref_reg,
    edition,
    editeur,
    source,
    observation,
    thematique,
    date,
    duree_validite,
    date_fin,
    temporalite,
    type
FROM regulations_cacem
WHERE 
  geom IS NOT NULL
  AND entity_name IS NOT NULL
  AND layer_name IS NOT NULL
  AND thematique IS NOT NULL
