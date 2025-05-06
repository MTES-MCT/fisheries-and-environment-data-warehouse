SELECT
    report_id,
    COALESCE(cfr, 'NO_CFR') AS cfr,
    external_immatriculation,
    ircs,
    vessel_name,
    vessel_id,
    flag_state,
    NULL AS trip_number,
    manual_prior_notifications.value->>'port' AS port_locode,
    p.port_name,
    p.latitude AS port_latitude,
    p.longitude AS port_longitude,
    created_at AT TIME ZONE 'UTC' AS operation_datetime_utc,
    created_at AT TIME ZONE 'UTC' AS report_datetime_utc,
    (manual_prior_notifications.value->>'predictedArrivalDatetimeUtc')::TIMESTAMPTZ AT TIME ZONE 'UTC' AS predicted_arrival_datetime_utc,
    (manual_prior_notifications.value->>'tripStartDate')::TIMESTAMPTZ AT TIME ZONE 'UTC' AS trip_start_date,
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
    (catch->>'weight')::DOUBLE PRECISION AS weight,
    'MANUAL' AS prior_notification_source
FROM manual_prior_notifications
LEFT JOIN ports p
ON p.locode = value->>'port'
JOIN jsonb_array_elements(value->'catchOnboard') catch ON true
WHERE
    created_at >= :min_date AND
    created_at < :max_date