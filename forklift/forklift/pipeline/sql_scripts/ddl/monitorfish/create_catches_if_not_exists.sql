CREATE TABLE IF NOT EXISTS monitorfish.catches (
    report_id String,
    cfr LowCardinality(String),
    flag_state LowCardinality(String),
    trip_number String,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    operation_datetime_utc DateTime,
    far_datetime_utc DateTime,
    fao_area LowCardinality(String),
    statistical_rectangle LowCardinality(String),
    economic_zone LowCardinality(String),
    gear LowCardinality(String),
    mesh Nullable(Float64),
    species LowCardinality(String),
    weight Nullable(Float64)
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(far_datetime_utc), cfr)
ORDER BY (toYear(far_datetime_utc), cfr, far_datetime_utc, )