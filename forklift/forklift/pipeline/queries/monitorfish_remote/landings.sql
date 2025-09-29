WITH lans AS (
    SELECT
        operation_type,
        report_id,
        referenced_report_id,
        cfr,
        flag_state,
        trip_number,
        operation_datetime_utc,
        activity_datetime_utc AS landing_datetime_utc,
        logbook_reports.value->>'port' AS port_locode,
        p.port_name,
        p.latitude AS port_latitude,
        p.longitude AS port_longitude,
        p.country_code_iso2,
        p.facade,
        p.region,
        catch->>'faoZone' AS fao_area,
        catch->>'statisticalRectangle' AS statistical_rectangle,
        catch->>'economicZone' AS economic_zone,
        catch->>'species' AS species,
        catch->>'nbFish' AS nb_fish, 
        catch->>'freshness' AS freshness, 
        catch->>'packaging' AS packaging, 
        catch->>'effortZone' AS effort_zone, 
        catch->>'presentation' AS presentation,
        catch->>'conversionFactor' AS conversion_factor,
        catch->>'preservationState' AS preservation_state,
        (catch->>'weight')::DOUBLE PRECISION AS weight
    FROM logbook_reports
    LEFT JOIN ports p
    ON p.locode = value->>'port'
    JOIN jsonb_array_elements(value->'catchLanded') catch ON true
    WHERE
        operation_datetime_utc >= :min_date AND
        operation_datetime_utc < :max_date AND
        log_type = 'LAN'
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
    report_id,
    cfr,
    flag_state,
    trip_number,
    operation_datetime_utc,
    landing_datetime_utc,
    port_locode,
    port_name,
    port_latitude,
    port_longitude,
    country_code_iso2,
    COALESCE(facade, 'Hors façade') AS facade,
    region,
    fao_area,
    statistical_rectangle,
    economic_zone,
    species,
    nb_fish, 
    freshness, 
    packaging, 
    effort_zone, 
    presentation,
    conversion_factor,
    preservation_state,
    weight
FROM lans
WHERE
    (
        report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
        OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
    )
    AND report_id NOT IN (
        SELECT referenced_report_id FROM acknowledged_cors_targeting_lans
        UNION ALL
        SELECT referenced_report_id FROM acknowledged_dels_targeting_lans
    ) AND
    weight IS NOT NULL AND
    species IS NOT NULL AND
    trip_number IS NOT NULL AND
    fao_area IS NOT NULL AND
    port_locode IS NOT NULL AND 
    landing_datetime_utc IS NOT NULL