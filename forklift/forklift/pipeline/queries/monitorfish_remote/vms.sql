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
    EXTRACT(epoch FROM time_since_previous_position) / 3600.0 AS hours_since_previous_position,
    average_speed,
    is_fishing,
    EXTRACT(epoch FROM time_emitting_at_sea) / 3600.0 AS hours_emitting_at_sea,
    network_type
FROM positions
WHERE
    date_time >= :min_date AND
    date_time < :max_date AND
    is_fishing IS NOT NULL
