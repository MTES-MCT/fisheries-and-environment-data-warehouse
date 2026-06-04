CREATE TABLE {database:Identifier}.{table:Identifier} (
    id Integer,
    geom String,
    url Nullable(String),
    layer_name String,
    facade LowCardinality(Nullable(String)),
    ref_reg Nullable(String),
    creation Nullable(DateTime),
    edition_bo Nullable(DateTime),
    edition_cacem Nullable(DateTime),
    editeur LowCardinality(Nullable(String)),
    source LowCardinality(Nullable(String)),
    observation Nullable(String),
    date Nullable(DateTime),
    date_fin Nullable(DateTime),
    type LowCardinality(Nullable(String)),
    resume Nullable(String),
    poly_name Nullable(String),
    plan LowCardinality(Nullable(String)),
    authorization_periods Nullable(String),
    prohibition_periods Nullable(String),
    row_hash String
)
ENGINE MergeTree
ORDER BY id
