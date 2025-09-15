CREATE TABLE {database:Identifier}.{table:Identifier} (
    id Int32,
    geom String,
    mpa_oriname String,
    des_desigfr String,
    row_hash Nullable(String),
    mpa_type Nullable(String),
    ref_reg Nullable(String),
    url_legicem Nullable(String),
    surface_area_km2 Float64
)
ENGINE MergeTree
ORDER BY mpa_oriname
