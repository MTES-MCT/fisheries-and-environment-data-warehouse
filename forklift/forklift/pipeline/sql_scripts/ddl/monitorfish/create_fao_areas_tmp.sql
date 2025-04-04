CREATE TABLE {database:Identifier}.{table:Identifier} (
    wkb_geometry String,
    f_code String,
    f_level Nullable(String),
    f_status Nullable(Float64),
    ocean Nullable(String),
    subocean Nullable(String),
    f_area Nullable(String),
    f_subarea Nullable(String),
    f_division Nullable(String),
    f_subdivis Nullable(String),
    f_subunit Nullable(String),
    name_en Nullable(String),
    name_fr Nullable(String),
    name_es Nullable(String),
    facade LowCardinality(String)
)
ENGINE MergeTree
ORDER BY f_code