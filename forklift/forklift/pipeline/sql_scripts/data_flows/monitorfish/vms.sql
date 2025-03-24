INSERT INTO monitorfish.vms
SELECT
    id,
    internal_reference_number AS cfr,
    external_reference_number,
    ircs,
    vessel_name,
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
    geoToH3(longitude, latitude, 6) AS h3_6,
    geoToH3(longitude, latitude, 8) AS h3_8
FROM monitorfish_proxy.positions
WHERE
    date_time >= {min_date:DateTime} AND
    date_time < {max_date:DateTime} AND
    is_fishing IS NOT NULL
