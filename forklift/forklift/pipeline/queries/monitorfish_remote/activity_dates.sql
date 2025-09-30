WITH activities AS (
    SELECT
        operation_datetime_utc,
        cfr,
        activity_datetime_utc,
        log_type,
        trip_number,
        trip_number_was_computed,
        report_id,
        flag_state
    FROM
        logbook_reports
    WHERE
        operation_datetime_utc >= :min_date AND
        operation_datetime_utc < :max_date AND
        log_type IS NOT NULL AND
        trip_number IS NOT NULL
),

dels_targeting_activities AS (
   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, activities.flag_state
   FROM logbook_reports del
   JOIN activities
   ON del.referenced_report_id = activities.report_id
   WHERE
        del.operation_type = 'DEL'
        AND del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '2 years'
),

cors_targeting_activities AS (
   SELECT cor.referenced_report_id, cor.report_id, cor.flag_state
   FROM logbook_reports cor
   JOIN activities
   ON cor.referenced_report_id = activities.report_id
   WHERE
        cor.operation_type = 'COR'
        AND cor.operation_datetime_utc >= :min_date
        AND cor.operation_datetime_utc < :max_date + INTERVAL '2 years'

),

acknowledged_report_ids AS (
   SELECT DISTINCT referenced_report_id
   FROM logbook_reports
   WHERE
       operation_datetime_utc >= :min_date
       AND operation_datetime_utc < :max_date + INTERVAL '2 years'
       AND operation_type = 'RET'
       AND value->>'returnStatus' = '000'
       AND referenced_report_id IN (
           SELECT operation_number FROM dels_targeting_activities
           UNION ALL
           SELECT report_id FROM cors_targeting_activities
           UNION ALL
           SELECT report_id FROM activities
       )
),

acknowledged_dels_targeting_activities AS (
    SELECT referenced_report_id
    FROM dels_targeting_activities
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_activities AS (
    SELECT referenced_report_id
    FROM cors_targeting_activities
    WHERE
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
)

SELECT
    operation_datetime_utc,
    cfr,
    activity_datetime_utc,
    log_type,
    trip_number,
    trip_number_was_computed,
    report_id,
    report_id IN (SELECT referenced_report_id FROM acknowledged_dels_targeting_activities) AS is_deleted,
    report_id IN (SELECT referenced_report_id FROM acknowledged_cors_targeting_activities) AS is_corrected,
    (
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    ) AS is_acknowledged
FROM activities