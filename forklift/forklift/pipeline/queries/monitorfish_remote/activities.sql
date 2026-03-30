WITH lans AS (
    SELECT
        referenced_report_id,
        operation_type,
        flag_state,
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
),

lan_reports AS (SELECT DISTINCT report_id, referenced_report_id, operation_type, flag_state FROM lans),

dels_targeting_lans AS (

   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, lan_reports.flag_state
   FROM logbook_reports del
   JOIN lan_reports
   ON del.referenced_report_id = lan_reports.report_id
   WHERE
        del.operation_type = 'DEL'
        AND del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '3 months'
),

cors_targeting_lans AS (
   SELECT cor.referenced_report_id, cor.report_id, cor.flag_state
   FROM logbook_reports cor
   JOIN lan_reports
   ON cor.referenced_report_id = lan_reports.report_id
   WHERE
        cor.operation_type = 'COR'
        AND cor.operation_datetime_utc >= :min_date
        AND cor.operation_datetime_utc < :max_date + INTERVAL '3 months'

),

acknowledged_report_ids AS (
   SELECT DISTINCT referenced_report_id
   FROM logbook_reports
   WHERE
       operation_datetime_utc >= :min_date
       AND operation_datetime_utc < :max_date + INTERVAL '3 months'
       AND operation_type = 'RET'
       AND value->>'returnStatus' = '000'
       AND referenced_report_id IN (
           SELECT operation_number FROM dels_targeting_lans
           UNION ALL
           SELECT report_id FROM cors_targeting_lans
           UNION ALL
           SELECT report_id FROM lan_reports
       )
),

acknowledged_dels_targeting_lans AS (
    SELECT referenced_report_id
    FROM dels_targeting_lans
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_lans AS (
    SELECT referenced_report_id
    FROM cors_targeting_lans
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
    CASE WHEN (
            (
                report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
                OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
            )
            AND report_id NOT IN (
                SELECT referenced_report_id FROM acknowledged_cors_targeting_lans
                UNION ALL
                SELECT referenced_report_id FROM acknowledged_dels_targeting_lans
            )
        ) THEN 'ACCEPTED'
        ELSE 'REJECTED' 
    END AS status
FROM lans
