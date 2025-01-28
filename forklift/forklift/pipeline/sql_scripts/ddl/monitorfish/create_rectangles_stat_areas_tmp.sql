CREATE TABLE {database:Identifier}.{table:Identifier} (
    ogc_fid Int32,
    wkb_geometry String,
    id Float64,
    icesname Nullable(String),
    south Nullable(Float64),
    west Nullable(Float64),
    north Nullable(Float64),
    east Nullable(Float64),
    area_km2 Nullable(Float64)
)
ENGINE MergeTree
ORDER BY id