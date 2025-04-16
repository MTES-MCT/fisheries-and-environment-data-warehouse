CREATE TABLE monitorenv.regulations_cacem_h3 (
    id Integer CODEC(Delta, ZSTD),
    h3 UInt64 CODEC(Delta, ZSTD),
    entity_name LowCardinality(String),
    layer_name LowCardinality(String),
    facade LowCardinality(String),
    type LowCardinality(String),
    PROJECTION monitorenv_regulations_cacem_h3_projection (
        SELECT * 
        ORDER BY
            facade,
            type,
            layer_name,
            entity_name,
            id
    )
)
ENGINE MergeTree
ORDER BY h3