CREATE TABLE IF NOT EXISTS monitorfish.activity_dates (
    operation_datetime_utc DateTime,
    cfr LowCardinality(String),
    activity_datetime_utc DateTime,
    log_type LowCardinality(String),
    trip_number String,
    trip_number_was_computed bool,
    report_id String,
    is_deleted bool,
    is_corrected bool,
    is_acknowledged bool
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY (cfr)
ORDER BY (cfr, activity_datetime_utc)