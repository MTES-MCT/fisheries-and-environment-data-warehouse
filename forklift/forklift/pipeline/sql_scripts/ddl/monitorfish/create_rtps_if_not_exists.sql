CREATE TABLE IF NOT EXISTS monitorfish.rtps (
    report_id String,
    cfr LowCardinality(String),
    external_immatriculation LowCardinality(Nullable(String)),
    ircs LowCardinality(Nullable(String)),
    vessel_name LowCardinality(Nullable(String)),
    vessel_id Nullable(Integer),
    flag_state LowCardinality(String),
    trip_number Nullable(String),
    port_locode LowCardinality(String),
    port_name LowCardinality(Nullable(String)),
    port_latitude Nullable(Float64),
    port_longitude Nullable(Float64),
    operation_datetime_utc DateTime,
    report_datetime_utc DateTime,
    return_datetime_utc DateTime,
    reason_of_return LowCardinality(String)
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(return_datetime_utc), cfr)
ORDER BY (toYear(return_datetime_utc), cfr, return_datetime_utc)