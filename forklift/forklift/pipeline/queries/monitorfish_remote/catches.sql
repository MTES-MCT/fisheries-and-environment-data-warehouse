WITH fars AS (
    SELECT
        operation_type,
        report_id,
        referenced_report_id,
        cfr,
        flag_state,
        trip_number,
        haul->'latitude' AS latitude,
        haul->'longitude' AS longitude,
        operation_datetime_utc,
        activity_datetime_utc AS far_datetime_utc,
        catch->>'faoZone' AS fao_area,
        catch->>'statisticalRectangle' AS statistical_rectangle,
        catch->>'economicZone' AS economic_zone,
        haul->>'gear' AS gear,
        (haul->>'mesh')::DOUBLE PRECISION AS mesh,
        catch->>'species' AS species,
        (catch->>'weight')::DOUBLE PRECISION AS weight
    FROM    
        logbook_reports,
        jsonb_array_elements(value->'hauls') haul,
        jsonb_array_elements(haul->'catches') catch
    WHERE
        operation_datetime_utc >= :min_date AND
        operation_datetime_utc < :max_date AND
        log_type = 'FAR'
),

far_reports AS (SELECT DISTINCT report_id, referenced_report_id, operation_type, flag_state FROM fars),

dels_targeting_fars AS (

   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, far_reports.flag_state
   FROM logbook_reports del
   JOIN far_reports
   ON del.referenced_report_id = far_reports.report_id
   WHERE
        del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '1 week'
        AND del.operation_type = 'DEL'
),

cors_targeting_fars AS (
   SELECT cor.referenced_report_id, cor.report_id, cor.flag_state
   FROM far_reports cor
   JOIN far_reports
   ON cor.referenced_report_id = far_reports.report_id
   WHERE cor.operation_type = 'COR'

),

acknowledged_report_ids AS (
   SELECT DISTINCT referenced_report_id
   FROM logbook_reports
   WHERE
       operation_datetime_utc >= :min_date
       AND operation_datetime_utc < :max_date + INTERVAL '1 week'
       AND operation_type = 'RET'
       AND value->>'returnStatus' = '000'
       AND referenced_report_id IN (
           SELECT operation_number FROM dels_targeting_fars
           UNION ALL
           SELECT report_id FROM cors_targeting_fars
           UNION ALL
           SELECT report_id FROM far_reports
       )
),

acknowledged_dels_targeting_fars AS (
    SELECT referenced_report_id
    FROM dels_targeting_fars
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_fars AS (
    SELECT referenced_report_id
    FROM cors_targeting_fars
    WHERE
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
)

SELECT
    report_id,
    cfr,
    flag_state,
    trip_number,
    latitude,
    longitude,
    operation_datetime_utc,
    far_datetime_utc,
    fao_area,
    statistical_rectangle,
    economic_zone,
    gear,
    mesh,
    species,
    weight
FROM fars
WHERE
    (
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    )
    AND report_id NOT IN (
        SELECT referenced_report_id FROM acknowledged_cors_targeting_fars
        UNION ALL
        SELECT referenced_report_id FROM acknowledged_dels_targeting_fars
    ) AND
    gear IS NOT NULL AND
    weight IS NOT NULL AND
    species IS NOT NULL AND
    trip_number IS NOT NULL AND
    fao_area IS NOT NULL