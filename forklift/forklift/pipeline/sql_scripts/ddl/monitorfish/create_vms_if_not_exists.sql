CREATE TABLE IF NOT EXISTS monitorfish.vms (
    id Integer,
    cfr LowCardinality(String),
    external_reference_number LowCardinality(String),
    ircs LowCardinality(String),
    vessel_name LowCardinality(String),
    flag_state LowCardinality(Nullable(String)),
    latitude Float CODEC(Delta, ZSTD),
    longitude Float CODEC(Delta, ZSTD),
    speed Nullable(Float) CODEC(Delta, ZSTD),
    course Nullable(Float) CODEC(Delta, ZSTD),
    date_time DateTime CODEC(DoubleDelta, ZSTD),
    is_manual boolean,
    is_at_port boolean,
    meters_from_previous_position Float CODEC(Delta, ZSTD),
    time_since_previous_position Float CODEC(Delta, ZSTD),
    average_speed Float CODEC(Delta, ZSTD),
    is_fishing boolean,
    time_emitting_at_sea Float CODEC(DoubleDelta, ZSTD),
    network_type LowCardinality(Nullable(String)),
    geometry Point,
    h3_6 UInt64 CODEC(Delta, ZSTD),
    h3_8 UInt64 CODEC(Delta, ZSTD)
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(date_time)
ORDER BY (cfr, date_time);