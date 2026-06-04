SELECT
    id,
    geom,
    mpa_oriname,
    des_desigfr,
    row_hash,
    mpa_type,
    ref_reg,
    url_legicem,
    ST_Area(geom::geography) / 1000000 AS surface_area_km2
FROM amp_cacem