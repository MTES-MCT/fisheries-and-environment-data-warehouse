CREATE TABLE {database:Identifier}.{table:Identifier} (
    facade LowCardinality(String),
    geometry String,
    id Int32
)
ENGINE MergeTree
ORDER BY facade