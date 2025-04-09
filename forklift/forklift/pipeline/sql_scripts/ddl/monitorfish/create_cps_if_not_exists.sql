CREATE TABLE IF NOT EXISTS monitorfish.cps (
    report_id String,
    cfr LowCardinality(String),
    flag_state LowCardinality(String),
    trip_number String,
    operation_datetime_utc DateTime,
    cps_datetime_utc DateTime,
    fao_area LowCardinality(String),
    statistical_rectangle LowCardinality(Nullable(String)),
    economic_zone LowCardinality(Nullable(String)),
    species LowCardinality(String),
    weight Float64,
    nb_fish Nullable(Float64),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    sex LowCardinality(Nullable(String)),
    health_state LowCardinality(Nullable(String)),
    care_minutes Nullable(Float64),
    ring Nullable(String),
    fate LowCardinality(Nullable(String)),
    comment Nullable(String)
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(cps_datetime_utc), cfr)
ORDER BY (toYear(cps_datetime_utc), cfr, cps_datetime_utc)