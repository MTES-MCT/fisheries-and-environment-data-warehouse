CREATE TABLE monitorenv.amp_cacem_h3 (
    id Int32 CODEC(Delta, ZSTD),
    h3 UInt64 CODEC(Delta, ZSTD),
    mpa_oriname LowCardinality(String),
    des_desigfr LowCardinality(String),
    mpa_type LowCardinality(String),
    PROJECTION monitorenv_amp_cacem_h3_projection (
        SELECT *
        ORDER BY mpa_type, des_desigfr, mpa_oriname, id, h3
    )
)
ENGINE MergeTree
ORDER BY h3
