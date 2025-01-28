CREATE TABLE {database:Identifier}.{table:Identifier} (
    ogc_fid Int32,
    wkb_geometry String,
    "union" Nullable(String),
    territory1 String,
    iso_ter1 Nullable(String),
    sovereign1 Nullable(String),
    iso_sov1 String,
    area_km2 Nullable(Float64)
)
ENGINE MergeTree
ORDER BY iso_sov1
