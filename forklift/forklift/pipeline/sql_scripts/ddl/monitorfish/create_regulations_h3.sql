CREATE TABLE monitorfish.regulations_h3 (
    id Int32 CODEC(Delta, ZSTD),
    law_type LowCardinality(String),
    topic LowCardinality(String),
    zone LowCardinality(String),
    region LowCardinality(Nullable(String)),
    h3 UInt64 CODEC(Delta, ZSTD)
)
ENGINE MergeTree
ORDER BY (law_type, topic, id, h3);