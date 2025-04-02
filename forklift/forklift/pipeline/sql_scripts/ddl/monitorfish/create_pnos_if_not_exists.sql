
CREATE TABLE IF NOT EXISTS monitorfish.pnos (
    report_id String,
    cfr LowCardinality(String),
    flag_state LowCardinality(String),
    trip_number String,
    port_locode LowCardinality(String),
    port_name LowCardinality(Nullable(String)),
    port_latitude Nullable(Float64),
    port_longitude Nullable(Float64),
    operation_datetime_utc DateTime,
    report_datetime_utc DateTime,
    predicted_arrival_datetime_utc DateTime,
    trip_start_date DateTime,
    fao_area LowCardinality(String),
    statistical_rectangle LowCardinality(Nullable(String)),
    economic_zone LowCardinality(Nullable(String)),
    species LowCardinality(String),
    nb_fish Nullable(Float64),
    freshness LowCardinality(Nullable(String)),
    packaging LowCardinality(Nullable(String)),
    effort_zone LowCardinality(Nullable(String)),
    presentation LowCardinality(Nullable(String)),
    conversion_factor Nullable(Float64),
    preservation_state LowCardinality(Nullable(String)),
    weight Float64
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(predicted_arrival_datetime_utc), cfr)
ORDER BY (toYear(predicted_arrival_datetime_utc), cfr, predicted_arrival_datetime_utc)