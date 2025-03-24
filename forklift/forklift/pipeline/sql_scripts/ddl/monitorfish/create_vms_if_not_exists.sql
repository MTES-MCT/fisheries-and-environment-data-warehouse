CREATE TABLE IF NOT EXISTS monitorfish.vms (
    id Integer,
    cfr LowCardinality(Nullable(String)),
    external_reference_number LowCardinality(Nullable(String)),
    ircs LowCardinality(Nullable(String)),
    vessel_name LowCardinality(Nullable(String)),
    flag_state LowCardinality(Nullable(String)),
    latitude Float,
    longitude Float,
    speed Nullable(Float),
    course Nullable(Float),
    date_time DateTime,
    is_manual boolean,
    is_at_port boolean,
    meters_from_previous_position Float,
    time_since_previous_position Float,
    average_speed Float,
    is_fishing boolean,
    time_emitting_at_sea Float,
    network_type LowCardinality(Nullable(String)),
    geometry Point,
    h3_6 UInt64,
    h3_8 UInt64
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(date_time)
PRIMARY KEY h3_6
ORDER BY (h3_6, date_time);