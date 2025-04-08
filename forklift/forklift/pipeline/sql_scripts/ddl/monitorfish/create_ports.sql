CREATE TABLE {database:Identifier}.{table:Identifier} (
    locode String,
    country_code_iso2 LowCardinality(String),
    region LowCardinality(Nullable(String)),
    port_name String,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    facade LowCardinality(String),
    fao_areas Array(String),
    is_active bool,
    nearby_position_at_sea_longitude Nullable(Float64),
    nearby_position_at_sea_latitude Nullable(Float64)
)
ENGINE MergeTree
ORDER BY locode