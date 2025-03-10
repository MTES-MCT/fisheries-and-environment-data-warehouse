CREATE TABLE IF NOT EXISTS monitorfish.enriched_catches (
    id Int64,
    report_id String,
    cfr LowCardinality(String),
    flag_state LowCardinality(String),
    trip_number String,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    far_datetime_utc DateTime,
    fao_area LowCardinality(String),
    statistical_rectangle LowCardinality(Nullable(String)),
    economic_zone LowCardinality(Nullable(String)),
    gear LowCardinality(String),
    mesh Nullable(Float64),
    species LowCardinality(String),
    weight Float64,
    trip_main_catch_type Enum('DEMERSAL' = 1, 'PELAGIC' = 2),
    segment LowCardinality(String),
    landing_datetime_utc Nullable(DateTime),
    landing_port_locode LowCardinality(Nullable(String)),
    landing_port_name LowCardinality(Nullable(String)),
    landing_port_latitude Nullable(Float64),
    landing_port_longitude Nullable(Float64)
    -- TODO:
    -- catch_facade LowCardinality(String),
    -- landing_port_country LowCardinality(Nullable(String)),
    -- landing_facade LowCardinality(String),
    -- landing_region LowCardinality(Nullable(String))
)
ENGINE MergeTree()
PARTITION BY toISOYear(far_datetime_utc)
PRIMARY KEY (toStartOfMonth(far_datetime_utc), cfr)
ORDER BY (toStartOfMonth(far_datetime_utc), cfr, far_datetime_utc, gear);