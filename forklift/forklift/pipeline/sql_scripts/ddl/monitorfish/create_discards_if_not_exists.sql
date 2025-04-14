CREATE TABLE IF NOT EXISTS monitorfish.discards (
    id Int64,
    report_id String,
    cfr LowCardinality(String),
    flag_state LowCardinality(String),
    trip_number String,
    operation_datetime_utc DateTime,
    dis_datetime_utc DateTime,
    fao_area LowCardinality(String),
    statistical_rectangle LowCardinality(Nullable(String)),
    economic_zone LowCardinality(Nullable(String)),
    species LowCardinality(String),
    weight Float64,
    presentation LowCardinality(Nullable(String)),
    packaging LowCardinality(Nullable(String)),
    preservation_state LowCardinality(Nullable(String)),
    conversion_factor Nullable(Float64)
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(dis_datetime_utc), cfr)
ORDER BY (toYear(dis_datetime_utc), cfr, dis_datetime_utc)