CREATE TABLE IF NOT EXISTS monitorfish.catches (
    id Int64,
    report_id String,
    cfr LowCardinality(String),
    flag_state LowCardinality(String),
    trip_number String,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    operation_datetime_utc DateTime,
    far_datetime_utc DateTime,
    fao_area LowCardinality(String),
    statistical_rectangle LowCardinality(Nullable(String)),
    economic_zone LowCardinality(Nullable(String)),
    gear LowCardinality(String),
    mesh Nullable(Float64),
    species LowCardinality(String),
    weight Float64
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(far_datetime_utc), cfr)
ORDER BY (toYear(far_datetime_utc), cfr, far_datetime_utc)