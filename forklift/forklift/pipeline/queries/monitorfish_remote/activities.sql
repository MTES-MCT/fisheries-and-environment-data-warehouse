SELECT
    operation_datetime_utc,
    cfr,
    GREATEST(
        LEAST(
            activity_datetime_utc,
            '2106-01-01 00:00:00'::TIMESTAMP WITHOUT TIME ZONE
        ),
        '1970-01-01 00:00:00'::TIMESTAMP WITHOUT TIME ZONE
    ) AS activity_datetime_utc,
    log_type,
    trip_number,
    trip_number_was_computed,
    report_id
FROM
    logbook_reports
WHERE
    operation_datetime_utc >= :min_date AND
    operation_datetime_utc < :max_date AND
    log_type IS NOT NULL AND
    trip_number IS NOT NULL