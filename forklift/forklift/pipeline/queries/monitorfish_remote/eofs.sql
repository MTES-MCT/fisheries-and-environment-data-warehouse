WITH eofs AS (
    SELECT
        operation_type,
        operation_number,
        report_id,
        referenced_report_id,
        logbook_reports.cfr,
        logbook_reports.external_identification AS external_immatriculation,
        logbook_reports.ircs,
        logbook_reports.vessel_name,
        v.id AS vessel_id,
        logbook_reports.flag_state,
        trip_number,
        operation_datetime_utc,
        report_datetime_utc,
        activity_datetime_utc AS end_of_fishing_datetime_utc
    FROM logbook_reports
    LEFT JOIN vessels v
    ON v.cfr = logbook_reports.cfr
    WHERE
        operation_datetime_utc >= :min_date AND
        operation_datetime_utc < :max_date AND
        log_type = 'EOF'
),

eof_reports AS (SELECT DISTINCT report_id, operation_number, referenced_report_id, operation_type, flag_state FROM eofs),

dels_targeting_eofs AS (

   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, eof_reports.flag_state
   FROM logbook_reports del
   JOIN eof_reports
   ON del.referenced_report_id = eof_reports.report_id
   WHERE
        del.operation_type = 'DEL'
        AND del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '3 months'
),

cors_targeting_eofs AS (
   SELECT cor.referenced_report_id, cor.operation_number, cor.flag_state
   FROM logbook_reports cor
   JOIN eof_reports
   ON cor.referenced_report_id = eof_reports.report_id
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
           SELECT operation_number FROM dels_targeting_eofs
           UNION ALL
           SELECT operation_number FROM cors_targeting_eofs
           UNION ALL
           SELECT operation_number FROM eof_reports
       )
),

acknowledged_dels_targeting_eofs AS (
    SELECT referenced_report_id
    FROM dels_targeting_eofs
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_eofs AS (
    SELECT referenced_report_id
    FROM cors_targeting_eofs
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
)

SELECT
    report_id,
    cfr,
    external_immatriculation,
    ircs,
    vessel_name,
    vessel_id,
    flag_state,
    trip_number,
    operation_datetime_utc,
    report_datetime_utc,
    end_of_fishing_datetime_utc
FROM eofs
WHERE
    (
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    )
    AND report_id NOT IN (
        SELECT referenced_report_id FROM acknowledged_cors_targeting_eofs
        UNION ALL
        SELECT referenced_report_id FROM acknowledged_dels_targeting_eofs
    ) AND
    trip_number IS NOT NULL AND
    end_of_fishing_datetime_utc IS NOT NULL