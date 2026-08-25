CREATE TABLE {database:Identifier}.{table:Identifier} (
    ogc_fid Int32,
    wkb_geometry String,
    territory1 Nullable(String),
    iso_ter1 Nullable(String),
    sovereign1 Nullable(String),
    iso_sov1 Nullable(String),
    area_km2 Nullable(Float64)
)
ENGINE MergeTree
ORDER BY iso_sov1
