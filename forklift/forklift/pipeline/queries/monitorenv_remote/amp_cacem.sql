SELECT
    id,
    ST_MakeValid(ST_SimplifyPreserveTopology(geom, 0.0001)) AS geom,
    mpa_oriname,
    des_desigfr,
    COALESCE(mpa_type, 'NO_MPA_TYPE') AS mpa_type
FROM amp_cacem
WHERE
    geom IS NOT NULL
    AND mpa_oriname IS NOT NULL
    AND des_desigfr IS NOT NULL
