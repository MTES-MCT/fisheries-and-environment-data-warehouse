WITH diss AS (
    SELECT
        operation_type,
        report_id,
        referenced_report_id,
        cfr,
        flag_state,
        trip_number,
        operation_datetime_utc,
        activity_datetime_utc AS dis_datetime_utc,
        catch->>'faoZone' AS fao_area,
        catch->>'statisticalRectangle' AS statistical_rectangle,
        catch->>'economicZone' AS economic_zone,
        catch->>'species' AS species,
        (catch->>'weight')::DOUBLE PRECISION AS weight,
        (catch->>'presentation') AS presentation,
        (catch->>'packaging') AS packaging,
        (catch->>'preservationState') AS preservation_state,
        (catch->>'conversionFactor')::DOUBLE PRECISION AS conversion_factor
    FROM    
        logbook_reports,
        jsonb_array_elements(value->'catches') catch
    WHERE
        operation_datetime_utc >= :min_date AND
        operation_datetime_utc < :max_date AND
        log_type = 'DIS'
),

dis_reports AS (SELECT DISTINCT report_id, referenced_report_id, operation_type, flag_state FROM diss),

dels_targeting_diss AS (

   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, dis_reports.flag_state
   FROM logbook_reports del
   JOIN dis_reports
   ON del.referenced_report_id = dis_reports.report_id
   WHERE
        del.operation_type = 'DEL'
        AND del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '3 months'
),

cors_targeting_diss AS (
   SELECT cor.referenced_report_id, cor.report_id, cor.flag_state
   FROM logbook_reports cor
   JOIN dis_reports
   ON cor.referenced_report_id = dis_reports.report_id
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
           SELECT operation_number FROM dels_targeting_diss
           UNION ALL
           SELECT report_id FROM cors_targeting_diss
           UNION ALL
           SELECT report_id FROM dis_reports
       )
),

acknowledged_dels_targeting_diss AS (
    SELECT referenced_report_id
    FROM dels_targeting_diss
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_diss AS (
    SELECT referenced_report_id
    FROM cors_targeting_diss
    WHERE
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
)

SELECT
    report_id,
    cfr,
    flag_state,
    trip_number,
    operation_datetime_utc,
    dis_datetime_utc,
    fao_area,
    statistical_rectangle,
    economic_zone,
    species,
    weight,
    presentation,
    packaging,
    preservation_state,
    conversion_factor
FROM diss
WHERE
    (
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    )
    AND report_id NOT IN (
        SELECT referenced_report_id FROM acknowledged_cors_targeting_diss
        UNION ALL
        SELECT referenced_report_id FROM acknowledged_dels_targeting_diss
    ) AND
    weight IS NOT NULL AND
    species IS NOT NULL AND
    trip_number IS NOT NULL AND
    fao_area IS NOT NULL