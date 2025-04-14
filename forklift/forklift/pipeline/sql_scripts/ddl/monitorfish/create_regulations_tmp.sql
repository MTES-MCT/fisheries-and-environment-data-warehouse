CREATE TABLE {database:Identifier}.{table:Identifier} (
    id Int32,
    law_type LowCardinality(String),
    topic String,
    zone String,
    region LowCardinality(Nullable(String)),
    geometry_simplified String
)
ENGINE MergeTree
ORDER BY id
