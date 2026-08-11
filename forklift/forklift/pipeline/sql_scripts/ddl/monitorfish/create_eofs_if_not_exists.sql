
CREATE TABLE IF NOT EXISTS monitorfish.eofs (
    report_id String,
    cfr LowCardinality(String),
    external_immatriculation LowCardinality(Nullable(String)),
    ircs LowCardinality(Nullable(String)),
    vessel_name LowCardinality(Nullable(String)),
    vessel_id Nullable(Integer),
    flag_state LowCardinality(String),
    trip_number Nullable(String),
    operation_datetime_utc DateTime,
    report_datetime_utc DateTime,
    end_of_fishing_datetime_utc DateTime
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (toYear(end_of_fishing_datetime_utc), cfr)
ORDER BY (toYear(end_of_fishing_datetime_utc), cfr, end_of_fishing_datetime_utc)