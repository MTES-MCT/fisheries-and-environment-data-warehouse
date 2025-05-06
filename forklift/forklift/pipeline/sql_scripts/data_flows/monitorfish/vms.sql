-- INSERT INTO monitorfish.vms
SELECT
    id,
    COALESCE(internal_reference_number, 'NO_CFR') AS cfr,
    COALESCE(external_reference_number, 'NO_EXTERNAL_REFERENCE_NUMBER') AS external_reference_number,
    COALESCE(ircs, 'NO_IRCS') AS ircs,
    COALESCE(vessel_name, 'NO_VESSEL_NAME') AS vessel_name,
    flag_state,
    latitude,
    longitude,
    speed,
    course,
    date_time,
    is_manual,
    is_at_port,
    meters_from_previous_position,
    time_since_previous_position,
    average_speed,
    is_fishing,
    time_emitting_at_sea,
    network_type,
    (longitude, latitude)::Point AS geometry,
    geoToH3(longitude, latitude, 8) AS h3_8
FROM monitorfish_proxy.positions
WHERE
    date_time >= {min_date:DateTime} AND
    date_time < {max_date:DateTime} AND
    is_fishing IS NOT NULL
