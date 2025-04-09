WITH cpss AS (
    SELECT
        operation_type,
        report_id,
        referenced_report_id,
        cfr,
        flag_state,
        trip_number,
        operation_datetime_utc,
        activity_datetime_utc AS cps_datetime_utc,
        catch->>'faoZone' AS fao_area,
        catch->>'statisticalRectangle' AS statistical_rectangle,
        catch->>'economicZone' AS economic_zone,
        catch->>'species' AS species,
        (catch->>'weight')::DOUBLE PRECISION AS weight,
        (catch->>'nbFish')::DOUBLE PRECISION AS nb_fish,
        (logbook_reports.value->>'latitude')::DOUBLE PRECISION AS latitude,
        (logbook_reports.value->>'longitude')::DOUBLE PRECISION AS longitude,
        catch->>'sex' AS sex,
        catch->>'healthState' AS health_state,
        (catch->>'careMinutes')::DOUBLE PRECISION AS care_minutes,
        catch->>'ring' AS ring,
        catch->>'fate' AS fate,
        catch->>'comment' AS comment
    FROM    
        logbook_reports,
        jsonb_array_elements(value->'catches') catch
    WHERE
        operation_datetime_utc >= :min_date AND
        operation_datetime_utc < :max_date AND
        log_type = 'CPS'
),

cps_reports AS (SELECT DISTINCT report_id, referenced_report_id, operation_type, flag_state FROM cpss),

dels_targeting_cpss AS (

   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, cps_reports.flag_state
   FROM logbook_reports del
   JOIN cps_reports
   ON del.referenced_report_id = cps_reports.report_id
   WHERE
        del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '1 week'
        AND del.operation_type = 'DEL'
),

cors_targeting_cpss AS (
   SELECT cor.referenced_report_id, cor.report_id, cor.flag_state
   FROM cps_reports cor
   JOIN cps_reports
   ON cor.referenced_report_id = cps_reports.report_id
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
           SELECT operation_number FROM dels_targeting_cpss
           UNION ALL
           SELECT report_id FROM cors_targeting_cpss
           UNION ALL
           SELECT report_id FROM cps_reports
       )
),

acknowledged_dels_targeting_cpss AS (
    SELECT referenced_report_id
    FROM dels_targeting_cpss
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_cpss AS (
    SELECT referenced_report_id
    FROM cors_targeting_cpss
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
    cps_datetime_utc,
    fao_area,
    statistical_rectangle,
    economic_zone,
    species,
    weight,
    nb_fish,
    latitude,
    longitude,
    sex,
    health_state,
    care_minutes,
    ring,
    fate,
    comment
FROM cpss
WHERE
    (
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    )
    AND report_id NOT IN (
        SELECT referenced_report_id FROM acknowledged_cors_targeting_cpss
        UNION ALL
        SELECT referenced_report_id FROM acknowledged_dels_targeting_cpss
    ) AND
    weight IS NOT NULL AND
    species IS NOT NULL AND
    trip_number IS NOT NULL AND
    fao_area IS NOT NULL