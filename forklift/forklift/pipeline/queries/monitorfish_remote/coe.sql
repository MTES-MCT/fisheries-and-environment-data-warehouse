WITH coes AS (
    SELECT
        operation_type,
        report_id,
        referenced_report_id,
        cfr,
        flag_state,
        trip_number,
        operation_datetime_utc,
        CASE 
            WHEN log_type = 'NOT-COE' THEN 'NOTIFICATION'
            ELSE 'DECLARATION'
        END AS report_document_type,
        activity_datetime_utc AS entry_datetime_utc,
        (r.value->>'latitudeEntered')::DOUBLE PRECISION AS latitude_entered,
        (r.value->>'longitudeEntered')::DOUBLE PRECISION AS longitude_entered,
        (r.value->>'economicZoneEntered') AS economic_zone_entered,
        (r.value->>'effortZoneEntered') AS effort_zone_entered,
        (r.value->>'faoZoneEntered') AS fao_area_entered,
        (r.value->>'statisticalRectangleEntered') AS statistical_rectangle_entered,
        (r.value->>'targetSpeciesOnEntry') AS target_species_on_entry,
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
        log_type IN ('COE', 'NOT-COE')
),

coe_reports AS (SELECT DISTINCT report_id, referenced_report_id, operation_type, flag_state FROM coes),

dels_targeting_coes AS (

   -- A DEL message has no flag_state, which we need to acknowledge messages of non french vessels.
   -- So we use the flag_state of the deleted message.
   SELECT del.referenced_report_id, del.operation_number, coe_reports.flag_state
   FROM logbook_reports del
   JOIN coe_reports
   ON del.referenced_report_id = coe_reports.report_id
   WHERE
        del.operation_type = 'DEL'
        AND del.operation_datetime_utc >= :min_date
        AND del.operation_datetime_utc < :max_date + INTERVAL '3 months'
),

cors_targeting_coes AS (
   SELECT cor.referenced_report_id, cor.report_id, cor.flag_state
   FROM logbook_reports cor
   JOIN coe_reports
   ON cor.referenced_report_id = coe_reports.report_id
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
           SELECT operation_number FROM dels_targeting_coes
           UNION ALL
           SELECT report_id FROM cors_targeting_coes
           UNION ALL
           SELECT report_id FROM coe_reports
       )
),

acknowledged_dels_targeting_coes AS (
    SELECT referenced_report_id
    FROM dels_targeting_coes
    WHERE
        operation_number IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN') -- flag_states for which we received RET messages
),

acknowledged_cors_targeting_coes AS (
    SELECT referenced_report_id
    FROM cors_targeting_coes
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
    entry_datetime_utc,
    latitude_entered,
    longitude_entered,
    economic_zone_entered,
    effort_zone_entered,
    fao_area_entered,
    statistical_rectangle_entered,
    target_species_on_entry,
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
FROM coes
WHERE
    (
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    )
    AND report_id NOT IN (
        SELECT referenced_report_id FROM acknowledged_cors_targeting_coes
        UNION ALL
        SELECT referenced_report_id FROM acknowledged_dels_targeting_coes
    ) AND
    trip_number IS NOT NULL AND
    fao_area_entered IS NOT NULL AND
    latitude_entered IS NOT NULL AND
    longitude_entered IS NOT NULL