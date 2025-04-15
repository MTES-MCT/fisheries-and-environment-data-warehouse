CREATE TABLE monitorfish.regulations_h3 (
    id Int32 CODEC(Delta, ZSTD),
    law_type LowCardinality(String),
    topic LowCardinality(String),
    zone LowCardinality(String),
    region LowCardinality(Nullable(String)),
    h3 UInt64 CODEC(Delta, ZSTD),
    PROJECTION monitorfish_regulations_h3_projection (
        SELECT *
        ORDER BY law_type, topic, id, h3
    )
)
ENGINE MergeTree
ORDER BY (h3, law_type, topic, id);