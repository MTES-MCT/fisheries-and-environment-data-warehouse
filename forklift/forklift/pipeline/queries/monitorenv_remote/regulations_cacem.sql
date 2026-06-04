SELECT
    id,
    geom,
    url,
    layer_name,
    facade,
    ref_reg,
    creation,
    edition_bo,
    edition_cacem,
    editeur,
    source,
    observation,
    date,
    date_fin,
    type,
    resume,
    poly_name,
    plan,
    authorization_periods,
    prohibition_periods,
    row_hash
FROM regulatory_areas
WHERE 
  geom IS NOT NULL
  AND layer_name IS NOT NULL

