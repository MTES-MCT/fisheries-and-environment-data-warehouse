WITH rtps AS (
    SELECT
        operation_type,
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
        activity_datetime_utc AS return_datetime_utc,
        logbook_reports.value->>'port' AS port_locode,
        p.port_name,
        p.latitude AS port_latitude,
        p.longitude AS port_longitude,
        COALESCE(logbook_reports.value->>'reasonOfReturn', 'UNK') AS reason_of_return
    FROM logbook_reports
    LEFT JOIN vessels v
    ON v.cfr = logbook_reports.cfr
    LEFT JOIN ports p
    ON p.locode = value->>'port'
    WHERE
        operation_datetime_utc >= :min_date AND
        operation_datetime_utc < :max_date AND
        log_type = 'RTP'
),

rtp_reports AS (SELECT DISTINCT report_id, referenced_report_id, operation_type, flag_state FROM rtps),

dels_targeting_rtps AS (

   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, rtp_reports.flag_state
   FROM logbook_reports del
   JOIN rtp_reports
   ON del.referenced_report_id = rtp_reports.report_id
   WHERE
        del.operation_type = 'DEL'
        AND del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '3 months'
),

cors_targeting_rtps AS (
   SELECT cor.referenced_report_id, cor.report_id, cor.flag_state
   FROM logbook_reports cor
   JOIN rtp_reports
   ON cor.referenced_report_id = rtp_reports.report_id
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
           SELECT operation_number FROM dels_targeting_rtps
           UNION ALL
           SELECT report_id FROM cors_targeting_rtps
           UNION ALL
           SELECT report_id FROM rtp_reports
       )
),

acknowledged_dels_targeting_rtps AS (
    SELECT referenced_report_id
    FROM dels_targeting_rtps
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_rtps AS (
    SELECT referenced_report_id
    FROM cors_targeting_rtps
    WHERE
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
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
    port_locode,
    port_name,
    port_latitude,
    port_longitude,
    operation_datetime_utc,
    report_datetime_utc,
    return_datetime_utc,
    reason_of_return
FROM rtps
WHERE
    (
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    )
    AND report_id NOT IN (
        SELECT referenced_report_id FROM acknowledged_cors_targeting_rtps
        UNION ALL
        SELECT referenced_report_id FROM acknowledged_dels_targeting_rtps
    ) AND
    port_locode IS NOT NULL AND
    return_datetime_utc IS NOT NULL AND 
    reason_of_return IS NOT NULL