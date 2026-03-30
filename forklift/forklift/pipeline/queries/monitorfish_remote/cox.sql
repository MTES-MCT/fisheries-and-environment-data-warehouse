WITH coxs AS (
    SELECT
        operation_type,
        report_id,
        referenced_report_id,
        cfr,
        flag_state,
        trip_number,
        operation_datetime_utc,
        CASE 
            WHEN log_type = 'NOT-COX' THEN 'NOTIFICATION'
            ELSE 'DECLARATION'
        END AS report_document_type,
        activity_datetime_utc AS exit_datetime_utc,
        (r.value->>'latitudeExited')::DOUBLE PRECISION AS latitude_exited,
        (r.value->>'longitudeExited')::DOUBLE PRECISION AS longitude_exited,
        (r.value->>'economicZoneExited') AS economic_zone_exited,
        (r.value->>'effortZoneExited') AS effort_zone_exited,
        (r.value->>'faoZoneExited') AS fao_area_exited,
        (r.value->>'statisticalRectangleExited') AS statistical_rectangle_exited,
        (r.value->>'targetSpeciesOnExit') AS target_species_on_exit,
        catch->>'species' AS species,
        (catch->>'weight')::DOUBLE PRECISION AS weight,
        (catch->>'nbFish')::DOUBLE PRECISION AS nb_fish,
        catch->>'faoZone' AS catch_fao_area,
        catch->>'statisticalRectangle' AS catch_statistical_rectangle,
        catch->>'economicZone' AS catch_economic_zone,
        catch->>'effortZone' AS catch_effort_zone,
        catch->>'presentation' AS presentation,
        catch->>'packaging' AS packaging,
        catch->>'freshness' AS freshness,
        catch->>'preservationState' AS preservation_state,
        (catch->>'conversionFactor')::DOUBLE PRECISION AS conversion_factor
    FROM logbook_reports r
    LEFT JOIN jsonb_array_elements(NULLIF(r.value->'speciesOnboard', 'null'::jsonb)) catch ON true
    WHERE
        operation_datetime_utc >= :min_date AND
        operation_datetime_utc < :max_date AND
        log_type IN ('COX', 'NOT-COX')
),

cox_reports AS (SELECT DISTINCT report_id, referenced_report_id, operation_type, flag_state FROM coxs),

dels_targeting_coxs AS (

   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, cox_reports.flag_state
   FROM logbook_reports del
   JOIN cox_reports
   ON del.referenced_report_id = cox_reports.report_id
   WHERE
        del.operation_type = 'DEL'
        AND del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '3 months'
),

cors_targeting_coxs AS (
   SELECT cor.referenced_report_id, cor.report_id, cor.flag_state
   FROM logbook_reports cor
   JOIN cox_reports
   ON cor.referenced_report_id = cox_reports.report_id
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
           SELECT operation_number FROM dels_targeting_coxs
           UNION ALL
           SELECT report_id FROM cors_targeting_coxs
           UNION ALL
           SELECT report_id FROM cox_reports
       )
),

acknowledged_dels_targeting_coxs AS (
    SELECT referenced_report_id
    FROM dels_targeting_coxs
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_coxs AS (
    SELECT referenced_report_id
    FROM cors_targeting_coxs
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
    report_document_type,
    exit_datetime_utc,
    latitude_exited,
    longitude_exited,
    economic_zone_exited,
    effort_zone_exited,
    fao_area_exited,
    statistical_rectangle_exited,
    target_species_on_exit,
    species,
    weight,
    nb_fish,
    catch_fao_area,
    catch_statistical_rectangle,
    catch_economic_zone,
    catch_effort_zone,
    presentation,
    packaging,
    freshness,
    preservation_state,
    conversion_factor
FROM coxs
WHERE
    (
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    )
    AND report_id NOT IN (
        SELECT referenced_report_id FROM acknowledged_cors_targeting_coxs
        UNION ALL
        SELECT referenced_report_id FROM acknowledged_dels_targeting_coxs
    ) AND
    trip_number IS NOT NULL AND
    fao_area_exited IS NOT NULL AND
    latitude_exited IS NOT NULL AND
    longitude_exited IS NOT NULL