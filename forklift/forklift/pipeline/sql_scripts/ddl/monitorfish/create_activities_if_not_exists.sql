CREATE TABLE IF NOT EXISTS monitorfish.activities (
    operation_datetime_utc DateTime,
    cfr LowCardinality(String),
    activity_datetime_utc Nullable(DateTime),
    log_type LowCardinality(String),
    trip_number String,
    trip_number_was_computed bool,
    report_id String,
    status Enum(
        'ACCEPTED' = 1,
        'REJECTED' = 2
    )
)
ENGINE MergeTree()
PARTITION BY toYYYYMM(operation_datetime_utc)
PRIMARY KEY cfr
ORDER BY cfr