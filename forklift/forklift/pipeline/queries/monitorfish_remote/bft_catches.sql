WITH fars AS (
    SELECT
        operation_number,
        operation_datetime_utc,
        report_id,
        operation_type,
        referenced_report_id,
        logbook_reports.cfr,
        logbook_reports.flag_state,
        trip_number,
        (haul->>'latitude')::DOUBLE PRECISION AS latitude,
        (haul->>'longitude')::DOUBLE PRECISION AS longitude,
        (haul->>'farDatetimeUtc')::TIMESTAMPTZ AT TIME ZONE 'UTC' AS far_datetime_utc,
        haul->>'gear' AS gear,
        (haul->>'mesh')::DOUBLE PRECISION As mesh
    FROM logbook_reports
    LEFT JOIN jsonb_array_elements(value->'hauls') haul ON true
    JOIN vessels v
    ON v.cfr = logbook_reports.cfr
    WHERE
        operation_datetime_utc >= :min_date
        AND operation_datetime_utc < :max_date
        AND log_type = 'FAR'
        AND vessel_type LIKE 'Senneur%'
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
),

catches_from_cvt AS (
    SELECT
        fars.*,
        (xpath(
            '//ers:CVT/attribute::IR',
            cvt,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::varchar AS quota_vessel_cfr,
        (xpath(
            '//ers:RAS/attribute::FA',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR || '.' || (xpath(
            '//ers:RAS/attribute::SA',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR  || '.' || (xpath(
            '//ers:RAS/attribute::ID',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR AS fao_area,
        (xpath(
            '//ers:RAS/attribute::EZ',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR AS economic_zone,
        (xpath(
            '//ers:SPE/attribute::SN',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR AS species,
        (xpath(
            '//ers:SPE/attribute::WT',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::varchar::DOUBLE PRECISION AS weight,
        (xpath(
            '//ers:SPE/attribute::WL',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::varchar::DOUBLE PRECISION AS weight_dead
    FROM logbook_raw_messages
    JOIN fars ON fars.operation_number = logbook_raw_messages.operation_number
    JOIN LATERAL unnest(xpath(
        '//ers:CVT',
        xml_message::xml,
        ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
    )) cvt ON true
    LEFT JOIN LATERAL unnest(xpath(
        '//ers:SPE',
        cvt,
        ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
    )) spe ON true
    WHERE
        (
            fars.report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
            OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
        )
        AND report_id NOT IN (
            SELECT referenced_report_id FROM acknowledged_cors_targeting_fars
            UNION ALL
            SELECT referenced_report_id FROM acknowledged_dels_targeting_fars
        )
),

catches_from_cvo AS (
    SELECT
        fars.*,
        (xpath(
            '//ers:CVO/attribute::IR',
            cvo,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::varchar AS quota_vessel_cfr,
        (xpath(
            '//ers:RAS/attribute::FA',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR || '.' || (xpath(
            '//ers:RAS/attribute::SA',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR  || '.' || (xpath(
            '//ers:RAS/attribute::ID',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR AS fao_area,
        (xpath(
            '//ers:RAS/attribute::EZ',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR AS economic_zone,
        (xpath(
            '//ers:SPE/attribute::SN',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::VARCHAR AS species,
        (xpath(
            '//ers:SPE/attribute::WT',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::varchar::DOUBLE PRECISION AS weight,
        (xpath(
            '//ers:SPE/attribute::WL',
            spe,
            ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
        ))[1]::varchar::DOUBLE PRECISION AS weight_dead
    FROM logbook_raw_messages
    JOIN fars ON fars.operation_number = logbook_raw_messages.operation_number
    JOIN LATERAL unnest(xpath(
        '//ers:CVO',
        xml_message::xml,
        ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
    )) cvo ON true
    LEFT JOIN LATERAL unnest(xpath(
        '//ers:SPE',
        cvo,
        ARRAY[ARRAY['ers', 'http://ec.europa.eu/fisheries/schema/ers/v3']]
    )) spe ON true
    WHERE
        (
            fars.report_id IN (SELECT referenced_report_id FROM acknowledged_report_ids)
            OR flag_state NOT IN ('FRA', 'GUF', 'VEN')
        )
        AND report_id NOT IN (
            SELECT referenced_report_id FROM acknowledged_cors_targeting_fars
            UNION ALL
            SELECT referenced_report_id FROM acknowledged_dels_targeting_fars
        )
),

catch_data AS (
    (
        SELECT *
        FROM catches_from_cvt
        WHERE cfr = quota_vessel_cfr AND weight + weight_dead > 0
    )
    UNION ALL
    (
        SELECT *
        FROM catches_from_cvo
        WHERE cfr = quota_vessel_cfr AND weight + weight_dead > 0
    )
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
    NULL AS statistical_rectangle,
    economic_zone,
    gear,
    mesh,
    species,
    weight
FROM catch_data
ORDER BY report_id